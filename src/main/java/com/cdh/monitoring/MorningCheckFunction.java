package com.cdh.monitoring;

import com.microsoft.azure.functions.ExecutionContext;
import com.microsoft.azure.functions.annotation.FunctionName;
import com.microsoft.azure.functions.annotation.TimerTrigger;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

/**
 * Morning Check – Semarchy xDM (Java Azure Function)
 *
 * Sections included (validated):
 *  - Jobs & Integration (mta_integ_batch)
 *  - Volume & Activity (Golden + errors + loads + golden by status)
 *  - Matching & Survivorship (DU_ACCOUNT + b_matchrule)
 *  - Data Notifications (mta_data_notif + mta_data_notif_log)
 *
 * Notes:
 *  - Adjust schema/table names if your DataLocation schema differs.
 *  - Ensure DbUtil.query() returns List<Map<String,Object>> and toHtmlTable() renders it.
 */
public class MorningCheckFunction {

    // ✅ Scheduling:
    //   - Production example: "0 30 7 * * *"  (07:30 UTC)
    //   - Quick test:         "0 */5 * * * *" (every 5 minutes)
    private static final String CRON = "0 */5 * * * *";

    // ---------- A) Jobs & Integration ----------
    private static final String SQL_JOBS_STATUS_COUNTS_24H = """
        select
          coalesce(mib.status,'(null)') as status,
          count(*) as nb
        from semarchy_repository.mta_integ_batch mib
        where mib.upddate >= now() - interval '24 hours'
        group by coalesce(mib.status,'(null)')
        order by nb desc;
        """;

    private static final String SQL_JOBS_STATUS_BY_DATALOC_24H = """
        select
          mdl."name" as datalocation,
          mib.status,
          count(*) as nb
        from semarchy_repository.mta_integ_batch mib
        join semarchy_repository.mta_data_location mdl
          on mdl."uuid" = mib.o_datalocation
        where mib.upddate >= now() - interval '24 hours'
        group by mdl."name", mib.status
        order by mdl."name", nb desc;
        """;

    private static final String SQL_JOBS_LAST_GLOBAL = """
        select
          mdl."name" as datalocation,
          mib.upddate,
          mib.status,
          mib.job_name,
          mib.batchid,
          left(coalesce(mib.job_message,''),400) as job_message
        from semarchy_repository.mta_integ_batch mib
        join semarchy_repository.mta_data_location mdl
          on mdl."uuid" = mib.o_datalocation
        order by mib.upddate desc
        limit 1;
        """;

    private static final String SQL_JOBS_LAST_PER_DATALOC = """
        select *
        from (
          select
            mdl."name" as datalocation,
            mib.upddate,
            mib.status,
            mib.job_name,
            mib.batchid,
            left(coalesce(mib.job_message,''),200) as job_message,
            row_number() over (partition by mdl."name" order by mib.upddate desc) as rn
          from semarchy_repository.mta_integ_batch mib
          join semarchy_repository.mta_data_location mdl
            on mdl."uuid" = mib.o_datalocation
        ) x
        where x.rn = 1
        order by x.upddate desc;
        """;

    // Thresholds (minutes/hours) can be configured via App Settings if you want
    private static final int STUCK_MINUTES = Integer.parseInt(getenv("STUCK_MINUTES", "60"));
    private static final int BLOCKED_HOURS = Integer.parseInt(getenv("BLOCKED_HOURS", "24"));

    private static String sqlJobsRunningPendingOverXMin(int minutes) {
        return """
            select
              mdl."name" as datalocation,
              mib.job_name,
              mib.status,
              mib.upddate,
              now() - mib.upddate as running_since,
              mib.batchid
            from semarchy_repository.mta_integ_batch mib
            join semarchy_repository.mta_data_location mdl
              on mdl."uuid" = mib.o_datalocation
            where mib.status in ('RUNNING','PENDING')
              and mib.upddate < now() - interval '%d minutes'
            order by mib.upddate asc;
            """.formatted(minutes);
    }

    private static String sqlJobsBlockedNotDoneOverXHours(int hours) {
        return """
            select
              mdl."name" as datalocation,
              mib.job_name,
              mib.status,
              mib.upddate,
              now() - mib.upddate as age,
              mib.batchid,
              left(coalesce(mib.job_message,''),400) as job_message
            from semarchy_repository.mta_integ_batch mib
            join semarchy_repository.mta_data_location mdl
              on mdl."uuid" = mib.o_datalocation
            where mib.status <> 'DONE'
              and mib.upddate < now() - interval '%d hours'
            order by mib.upddate asc;
            """.formatted(hours);
    }

    private static final String SQL_JOBS_FAILED_DETAILS_24H = """
        select
          mdl."name" as datalocation,
          mib.upddate,
          mib.status,
          mib.job_name,
          mib.batchid,
          left(coalesce(mib.job_message,''), 700) as job_message
        from semarchy_repository.mta_integ_batch mib
        join semarchy_repository.mta_data_location mdl
          on mdl."uuid" = mib.o_datalocation
        where mib.upddate >= now() - interval '24 hours'
          and mib.status in ('FAILED','ERROR','SUSPENDED')
        order by mib.upddate desc;
        """;

    // ---------- C) Volume & Activity (Account data location – adapt schema/table if needed) ----------
    private static final String SQL_GOLDEN_CREATED_YESTERDAY = """
        select count(*) as golden_created_yesterday
        from semarchy_data_location_account.md_account
        where b_credate >= date_trunc('day', now() - interval '1 day')
          and b_credate <  date_trunc('day', now());
        """;

    private static final String SQL_GOLDEN_UPDATED_YESTERDAY = """
        select count(*) as golden_updated_yesterday
        from semarchy_data_location_account.md_account
        where b_upddate >= date_trunc('day', now() - interval '1 day')
          and b_upddate <  date_trunc('day', now());
        """;

    private static final String SQL_RECORDS_IN_ERROR_24H = """
        select count(*) as records_in_error_24h
        from semarchy_data_location_account.sd_account
        where b_credate >= now() - interval '24 hours'
          and b_error_status is not null;
        """;

    private static final String SQL_LOADS_24H = """
        select count(*) as loads_24h
        from semarchy_repository.mta_integ_batch
        where upddate >= now() - interval '24 hours';
        """;

    private static final String SQL_GOLDEN_BY_STATUS = """
        select
          b_pubid,
          count(*) as nb
        from semarchy_data_location_account.md_account
        group by b_pubid
        order by nb desc;
        """;

    // ---------- Matching & Survivorship (DU_ACCOUNT + b_matchrule) ----------
    private static final String SQL_MATCH_SUSPECTS_BY_RULE_24H = """
        select
          b_matchrule as match_rule,
          count(*) as suspects_24h
        from semarchy_data_location_account.du_account
        where b_credate >= now() - interval '24 hours'
        group by b_matchrule
        order by suspects_24h desc;
        """;

    // Requires DU_ACCOUNT having b_groupid. If not present in your DU table, remove this query.
    private static final String SQL_MATCH_GROUPS_BY_RULE_24H = """
        select
          b_matchrule as match_rule,
          count(distinct b_matchscore) as match_groups,
          count(*) as suspects
        from semarchy_data_location_account.du_account
        where b_credate >= now() - interval '24 hours'
        group by b_matchrule
        order by suspects desc;
        """;

    private static final String SQL_MATCH_SUSPECTS_TOTAL_BY_RULE = """
        select
          b_matchrule as match_rule,
          count(*) as suspects_total
        from semarchy_data_location_account.du_account
        group by b_matchrule
        order by suspects_total desc;
        """;

    // ---------- Data Notifications (mta_data_notif + mta_data_notif_log) ----------
    private static final String SQL_NOTIF_LAST_GLOBAL = """
        select
          dn."name" as notif_name,
          dnl.execution_status,
          coalesce(dnl."timestamp", dnl.upddate, dnl.credate) as event_ts,
          dnl.attempt_count,
          dnl.record_count,
          dnl.message_count,
          dnl.query_duration,
          dnl.sending_duration,
          left(coalesce(dnl.error_message,''),600) as error_message
        from semarchy_repository.mta_data_notif_log dnl
        left join semarchy_repository.mta_data_notif dn
          on dn."uuid" = dnl.r_datanotif
        order by coalesce(dnl."timestamp", dnl.upddate, dnl.credate) desc
        limit 1;
        """;

    private static final String SQL_NOTIF_LAST_PER_NOTIF = """
        select *
        from (
          select
            dn."name" as notif_name,
            dnl.execution_status,
            coalesce(dnl."timestamp", dnl.upddate, dnl.credate) as event_ts,
            dnl.attempt_count,
            dnl.record_count,
            dnl.message_count,
            dnl.query_duration,
            dnl.sending_duration,
            left(coalesce(dnl.error_message,''),400) as error_message,
            row_number() over (
              partition by dn."uuid"
              order by coalesce(dnl."timestamp", dnl.upddate, dnl.credate) desc
            ) as rn
          from semarchy_repository.mta_data_notif dn
          left join semarchy_repository.mta_data_notif_log dnl
            on dnl.r_datanotif = dn."uuid"
        ) x
        where rn = 1
        order by event_ts desc nulls last;
        """;

    private static final String SQL_NOTIF_STATUS_COUNTS_24H = """
        select
          coalesce(dnl.execution_status,'(null)') as execution_status,
          count(*) as nb
        from semarchy_repository.mta_data_notif_log dnl
        where coalesce(dnl."timestamp", dnl.upddate, dnl.credate) >= now() - interval '24 hours'
        group by coalesce(dnl.execution_status,'(null)')
        order by nb desc;
        """;

    private static final String SQL_NOTIF_ERRORS_24H = """
        select
          dn."name" as notif_name,
          dnl.execution_status,
          coalesce(dnl."timestamp", dnl.upddate, dnl.credate) as event_ts,
          dnl.attempt_count,
          dnl.record_count,
          dnl.message_count,
          left(coalesce(dnl.error_message,''),800) as error_message
        from semarchy_repository.mta_data_notif_log dnl
        join semarchy_repository.mta_data_notif dn
          on dn."uuid" = dnl.r_datanotif
        where coalesce(dnl."timestamp", dnl.upddate, dnl.credate) >= now() - interval '24 hours'
          and (
            dnl.execution_status in ('FAILED','ERROR')
            or dnl.error_message is not null
          )
        order by event_ts desc;
        """;

    // ---------- Azure Function entry point ----------
    @FunctionName("MorningCheckSemarchy")
    public void run(
            @TimerTrigger(name = "timerInfo", schedule = CRON) String timerInfo,
            final ExecutionContext context
    ) {
        var log = context.getLogger();
        log.info("MorningCheck START - timerInfo=" + timerInfo);

        try {
            // 1) Read configuration
            String envName = getenv("ENV_NAME", "DEV");

            String dbHost = mustGet("DB_HOST");
            String dbPort = getenv("DB_PORT", "5432"); // ✅ fixed default
            String dbName = mustGet("DB_NAME");
            String dbUser = mustGet("DB_USER");
            String dbPass = mustGet("DB_PASS");

            String smtpHost = mustGet("SMTP_HOST");
            int smtpPort = Integer.parseInt(getenv("SMTP_PORT", "587"));
            String smtpUser = mustGet("SMTP_USER");
            String smtpPass = mustGet("SMTP_PASS");
            String mailFrom = mustGet("MAIL_FROM");
            String mailTo = mustGet("MAIL_TO");

            String jdbcUrl = "jdbc:postgresql://" + dbHost + ":" + dbPort + "/" + dbName + "?sslmode=require";

            // Optional: quick DNS log (helps when moving between public/private)
            logDnsResolution(log, dbHost);

            // 2) Execute all KPI queries (ordered)
            Map<String, List<Map<String, Object>>> sections = new LinkedHashMap<>();

            // A) Jobs & Integration
            sections.put("🟦 Jobs – Compteurs (24h)", DbUtil.query(jdbcUrl, dbUser, dbPass, SQL_JOBS_STATUS_COUNTS_24H));
            sections.put("🟦 Jobs – Compteurs par DataLocation (24h)", DbUtil.query(jdbcUrl, dbUser, dbPass, SQL_JOBS_STATUS_BY_DATALOC_24H));
            sections.put("🟦 Jobs – Dernier job (global)", DbUtil.query(jdbcUrl, dbUser, dbPass, SQL_JOBS_LAST_GLOBAL));
            sections.put("🟦 Jobs – Dernier job par DataLocation", DbUtil.query(jdbcUrl, dbUser, dbPass, SQL_JOBS_LAST_PER_DATALOC));
            sections.put("🟦 Jobs – RUNNING/PENDING > " + STUCK_MINUTES + " min", DbUtil.query(jdbcUrl, dbUser, dbPass, sqlJobsRunningPendingOverXMin(STUCK_MINUTES)));
            sections.put("🟦 Jobs – Bloqués != DONE > " + BLOCKED_HOURS + " h", DbUtil.query(jdbcUrl, dbUser, dbPass, sqlJobsBlockedNotDoneOverXHours(BLOCKED_HOURS)));
            List<Map<String, Object>> jobErrors = DbUtil.query(jdbcUrl, dbUser, dbPass, SQL_JOBS_FAILED_DETAILS_24H);
            sections.put("🟥 Jobs – Anomalies (FAILED/ERROR/SUSPENDED) (24h)", jobErrors);

            // C) Volume & activity
            sections.put("🟩 Volume – Golden créés hier", DbUtil.query(jdbcUrl, dbUser, dbPass, SQL_GOLDEN_CREATED_YESTERDAY));
            sections.put("🟩 Volume – Golden mis à jour hier", DbUtil.query(jdbcUrl, dbUser, dbPass, SQL_GOLDEN_UPDATED_YESTERDAY));
            sections.put("🟩 Volume – Records en erreur (24h)", DbUtil.query(jdbcUrl, dbUser, dbPass, SQL_RECORDS_IN_ERROR_24H));
            sections.put("🟩 Volume – Loads intégrés (24h)", DbUtil.query(jdbcUrl, dbUser, dbPass, SQL_LOADS_24H));
            sections.put("🟩 Golden – Répartition par statut", DbUtil.query(jdbcUrl, dbUser, dbPass, SQL_GOLDEN_BY_STATUS));

            // Matching & survivorship
            sections.put("🟨 Matching – Suspects par règle (24h)", DbUtil.query(jdbcUrl, dbUser, dbPass, SQL_MATCH_SUSPECTS_BY_RULE_24H));
            sections.put("🟨 Matching – Groupes par règle (24h)", DbUtil.query(jdbcUrl, dbUser, dbPass, SQL_MATCH_GROUPS_BY_RULE_24H));
            sections.put("🟨 Matching – Suspects total par règle", DbUtil.query(jdbcUrl, dbUser, dbPass, SQL_MATCH_SUSPECTS_TOTAL_BY_RULE));

            // Data notifications
            sections.put("🟪 Data Notifications – Dernier log (global)", DbUtil.query(jdbcUrl, dbUser, dbPass, SQL_NOTIF_LAST_GLOBAL));
            sections.put("🟪 Data Notifications – Dernier log par notification", DbUtil.query(jdbcUrl, dbUser, dbPass, SQL_NOTIF_LAST_PER_NOTIF));
            sections.put("🟪 Data Notifications – Compteurs par statut (24h)", DbUtil.query(jdbcUrl, dbUser, dbPass, SQL_NOTIF_STATUS_COUNTS_24H));
            List<Map<String, Object>> notifErrors = DbUtil.query(jdbcUrl, dbUser, dbPass, SQL_NOTIF_ERRORS_24H);
            sections.put("🟥 Data Notifications – Erreurs (24h)", notifErrors);

            // 3) Build HTML
            String now = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm"));
            boolean hasRed = hasRows(jobErrors) || hasRows(notifErrors);
            String emoji = hasRed ? "🔴" : "🟢";

            String html = buildHtml(envName, now, timerInfo, sections, hasRed);

            // 4) Send email
            String subject = emoji + " Morning Check Semarchy xDM – " + envName + " – " + now;
            MailUtil.sendHtml(smtpHost, smtpPort, smtpUser, smtpPass, mailFrom, mailTo, subject, html);

            log.info("MorningCheck OK - email sent. jobErrors=" + safeSize(jobErrors) + " notifErrors=" + safeSize(notifErrors));

        } catch (Exception e) {
            log.severe("MorningCheck FAILED: " + e.getClass().getName() + " - " + e.getMessage());
            Throwable c = e.getCause();
            int depth = 0;
            while (c != null && depth < 10) {
                log.severe("Caused by: " + c.getClass().getName() + " - " + c.getMessage());
                c = c.getCause();
                depth++;
            }
            e.printStackTrace();
        } finally {
            log.info("MorningCheck END");
        }
    }

    // ---------- HTML report ----------
    private static String buildHtml(String env,
                                    String now,
                                    String timerInfo,
                                    Map<String, List<Map<String, Object>>> sections,
                                    boolean hasRed) {

        String badge = hasRed
                ? "<span style='display:inline-block;padding:4px 10px;border-radius:999px;background:#ffe5e5;color:#a60000;font-weight:bold;'>ALERT</span>"
                : "<span style='display:inline-block;padding:4px 10px;border-radius:999px;background:#e7f7ea;color:#1b7f2a;font-weight:bold;'>OK</span>";

        StringBuilder sb = new StringBuilder();
        sb.append("<html><body style='font-family:Arial, sans-serif;'>");
        sb.append("<h2>📅 MORNING CHECK – Semarchy xDM ").append(badge).append("</h2>");
        sb.append("<p><b>Env:</b> ").append(escape(env)).append("<br/>")
          .append("<b>Date:</b> ").append(escape(now)).append("<br/>")
          .append("<b>Timer:</b> ").append(escape(timerInfo)).append("</p>");

        for (Map.Entry<String, List<Map<String, Object>>> e : sections.entrySet()) {
            sb.append("<h3 style='margin-top:18px;'>").append(escape(e.getKey())).append("</h3>");
            List<Map<String, Object>> rows = e.getValue();
            if (rows == null || rows.isEmpty()) {
                sb.append("<p style='color:#2e7d32;'>✅ Aucun élément</p>");
            } else {
                sb.append(DbUtil.toHtmlTable(rows));
            }
        }

        sb.append("<hr/>")
          .append("<p style='color:#666;font-size:12px;'>Generated by Azure Function (Java)</p>")
          .append("</body></html>");

        return sb.toString();
    }

    // ---------- Helpers ----------
    private static boolean hasRows(List<Map<String, Object>> rows) {
        return rows != null && !rows.isEmpty();
    }

    private static int safeSize(List<Map<String, Object>> rows) {
        return rows == null ? 0 : rows.size();
    }

    private static String mustGet(String key) {
        String v = System.getenv(key);
        if (v == null || v.isBlank()) throw new IllegalStateException("Missing app setting: " + key);
        return v.trim();
    }

    private static String getenv(String key, String def) {
        String v = System.getenv(key);
        return (v == null || v.isBlank()) ? def : v.trim();
    }

    private static String escape(String s) {
        if (s == null) return "";
        return s.replace("&", "&amp;")
                .replace("<", "&lt;")
                .replace(">", "&gt;")
                .replace("\"", "&quot;");
    }

    private static void logDnsResolution(java.util.logging.Logger log, String host) {
        try {
            var addrs = java.net.InetAddress.getAllByName(host);
            log.info("DNS OK for " + host + " => " + java.util.Arrays.toString(addrs));
        } catch (Exception ex) {
            log.warning("DNS FAIL for " + host + " => " + ex.getClass().getName() + " - " + ex.getMessage());
        }
    }
}
