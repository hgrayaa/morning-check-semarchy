package com.cdh.monitoring;

import com.microsoft.azure.functions.ExecutionContext;
import com.microsoft.azure.functions.annotation.FunctionName;
import com.microsoft.azure.functions.annotation.TimerTrigger;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

public class SemarchyAlertFunction {

    // Check toutes les 5 minutes
    private static final String CRON = "0 */5 * * * *";

    private static final int ENGINE_IDLE_MINUTES =
            Integer.parseInt(getenv("ENGINE_IDLE_MINUTES", "15"));

    private static final int JOB_BLOCKED_MINUTES =
            Integer.parseInt(getenv("JOB_BLOCKED_MINUTES", "15"));

    // 1) Jobs suspendus / bloqués
    private static String sqlBlockedJobs(int minutes) {
        return """
            select
              mdl."name" as datalocation,
              mib.job_name,
              mib.status,
              mib.upddate,
              now() - mib.upddate as age,
              mib.batchid,
              left(coalesce(mib.job_message,''), 600) as job_message
            from semarchy_repository.mta_integ_batch mib
            join semarchy_repository.mta_data_location mdl
              on mdl."uuid" = mib.o_datalocation
            where mib.status in ('SUSPENDED','FAILED','ERROR','RUNNING','PENDING')
              and mib.upddate < now() - interval '%d minutes'
            order by mib.upddate asc;
            """.formatted(minutes);
    }

    // 2) Data notifications en erreur / suspendues
    private static final String SQL_DATA_NOTIF_ERRORS = """
        select
          dn."name" as notif_name,
          dnl.execution_status,
          coalesce(dnl."timestamp", dnl.upddate, dnl.credate) as event_ts,
          dnl.attempt_count,
          dnl.record_count,
          dnl.message_count,
          left(coalesce(dnl.error_message,''), 800) as error_message
        from semarchy_repository.mta_data_notif_log dnl
        join semarchy_repository.mta_data_notif dn
          on dn."uuid" = dnl.r_datanotif
        where coalesce(dnl."timestamp", dnl.upddate, dnl.credate) >= now() - interval '15 minutes'
          and (
            dnl.execution_status in ('FAILED','ERROR','SUSPENDED')
            or dnl.error_message is not null
          )
        order by event_ts desc;
        """;

    // 3) Engine probablement arrêté : aucun batch récent
    private static String sqlEngineProbablyStopped(int minutes) {
        return """
            select
              max(upddate) as last_batch_execution,
              now() - max(upddate) as age,
              case
                when max(upddate) is null then 'NO_BATCH_FOUND'
                when max(upddate) < now() - interval '%d minutes' then 'ENGINE_PROBABLY_STOPPED'
                else 'OK'
              end as engine_status
            from semarchy_repository.mta_integ_batch
            having max(upddate) is null
                or max(upddate) < now() - interval '%d minutes';
            """.formatted(minutes, minutes);
    }

    @FunctionName("SemarchyAlertMonitoring")
    public void run(
            @TimerTrigger(name = "timerInfo", schedule = CRON) String timerInfo,
            final ExecutionContext context
    ) {
        var log = context.getLogger();
        log.info("SemarchyAlertMonitoring START - timerInfo=" + timerInfo);

        try {
            String envName = getenv("ENV_NAME", "DEV");

            String dbHost = mustGet("DB_HOST");
            String dbPort = getenv("DB_PORT", "5432");
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

            List<Map<String, Object>> blockedJobs =
                    DbUtil.query(jdbcUrl, dbUser, dbPass, sqlBlockedJobs(JOB_BLOCKED_MINUTES));

            List<Map<String, Object>> notifErrors =
                    DbUtil.query(jdbcUrl, dbUser, dbPass, SQL_DATA_NOTIF_ERRORS);

            List<Map<String, Object>> engineStopped =
                    DbUtil.query(jdbcUrl, dbUser, dbPass, sqlEngineProbablyStopped(ENGINE_IDLE_MINUTES));

            boolean hasAlert =
                    hasRows(blockedJobs)
                    || hasRows(notifErrors)
                    || hasRows(engineStopped);

            if (!hasAlert) {
                log.info("No alert detected. No email sent.");
                return;
            }

            String now = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm"));

            String subject = "[ALERT] Semarchy xDM Monitoring - " + envName + " - " + now;

            String html = buildHtml(
                    envName,
                    now,
                    blockedJobs,
                    notifErrors,
                    engineStopped
            );

            MailUtil.sendHtml(
                    smtpHost,
                    smtpPort,
                    smtpUser,
                    smtpPass,
                    mailFrom,
                    mailTo,
                    subject,
                    html
            );

            log.info("Alert email sent. blockedJobs="
                    + blockedJobs.size()
                    + ", notifErrors="
                    + notifErrors.size()
                    + ", engineAlerts="
                    + engineStopped.size());

        } catch (Exception e) {
            log.severe("SemarchyAlertMonitoring FAILED: "
                    + e.getClass().getName()
                    + " - "
                    + e.getMessage());

            Throwable c = e.getCause();
            int depth = 0;

            while (c != null && depth < 10) {
                log.severe("Caused by: "
                        + c.getClass().getName()
                        + " - "
                        + c.getMessage());
                c = c.getCause();
                depth++;
            }

            e.printStackTrace();

        } finally {
            log.info("SemarchyAlertMonitoring END");
        }
    }

    private static String buildHtml(
            String env,
            String now,
            List<Map<String, Object>> blockedJobs,
            List<Map<String, Object>> notifErrors,
            List<Map<String, Object>> engineStopped
    ) {
        StringBuilder sb = new StringBuilder();

        sb.append("<html><body style='font-family:Arial, sans-serif;'>");

        sb.append("<h2 style='color:#a60000;'>")
          .append("[ALERT] Semarchy xDM Monitoring")
          .append("</h2>");

        sb.append("<p>")
          .append("<b>Env:</b> ").append(env).append("<br/>")
          .append("<b>Date:</b> ").append(now).append("<br/>")
          .append("<b>Fréquence:</b> toutes les 5 minutes")
          .append("</p>");

        if (hasRows(blockedJobs)) {
            sb.append("<h3>🟦 Jobs – Bloqués / Suspendus</h3>");
            sb.append(DbUtil.toHtmlTable(blockedJobs));
        }

        if (hasRows(notifErrors)) {
            sb.append("<h3>🟥 Data Notifications – Erreurs</h3>");
            sb.append(DbUtil.toHtmlTable(notifErrors));
        }

        if (hasRows(engineStopped)) {
            sb.append("<h3>🟥 Semarchy Engine probablement arrêté</h3>");
            sb.append("<p style='color:#a60000;'>")
              .append("Aucun batch récent détecté. Dernier batch exécuté il y a plus de ")
              .append(ENGINE_IDLE_MINUTES)
              .append(" minutes.")
              .append("</p>");
            sb.append(DbUtil.toHtmlTable(engineStopped));
        }

        sb.append("<hr/>")
          .append("<p style='color:#666;font-size:12px;'>")
          .append("Generated by Azure Function Java - Semarchy Alert Monitoring")
          .append("</p>");

        sb.append("</body></html>");

        return sb.toString();
    }

    private static boolean hasRows(List<Map<String, Object>> rows) {
        return rows != null && !rows.isEmpty();
    }

    private static String mustGet(String key) {
        String v = System.getenv(key);
        if (v == null || v.isBlank()) {
            throw new IllegalStateException("Missing app setting: " + key);
        }
        return v.trim();
    }

    private static String getenv(String key, String def) {
        String v = System.getenv(key);
        return (v == null || v.isBlank()) ? def : v.trim();
    }
}
