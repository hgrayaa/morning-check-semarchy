package com.cdh.monitoring;

import com.microsoft.azure.functions.ExecutionContext;
import com.microsoft.azure.functions.annotation.FunctionName;
import com.microsoft.azure.functions.annotation.TimerTrigger;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;

public class SemarchyAlertFunction {

    private static final String CRON = "0 */5 * * * *";

    private static final int ENGINE_IDLE_MINUTES =
            Integer.parseInt(getenv("ENGINE_IDLE_MINUTES", "15"));

    private static final int JOB_BLOCKED_MINUTES =
            Integer.parseInt(getenv("JOB_BLOCKED_MINUTES", "15"));

    private static String sqlBlockedJobs(int minutes) {
        return """
            select
              mdl."name" as datalocation,
              mib.job_name,
              mib.status,
              mib.upddate,
              now() - mib.upddate as age,
              mib.batchid,
              left(coalesce(mib.job_message,''), 250) as job_message
            from semarchy_repository.mta_integ_batch mib
            join semarchy_repository.mta_data_location mdl
              on mdl."uuid" = mib.o_datalocation
            where mib.status in ('SUSPENDED','FAILED')
            order by mib.upddate asc;
            """.formatted(minutes);
    }

    private static final String SQL_DATA_NOTIF_ERRORS = """
        select
          dn."name" as notif_name,
          dnl.execution_status,
          coalesce(dnl."timestamp", dnl.upddate, dnl.credate) as event_ts,
          dnl.attempt_count,
          dnl.record_count,
          dnl.message_count,
          left(coalesce(dnl.error_message,''), 350) as error_message
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

    private static String sqlEngineProbablyStopped(int minutes) {
        return """
            select
              max(upddate) as last_batch_execution,
              now() - max(upddate) as age,
              case
                when max(upddate) is null then 'NO_BATCH_FOUND'
                when max(upddate) < now() - (%d * interval '1 minute') then 'ENGINE_PROBABLY_STOPPED'
                else 'OK'
              end as engine_status
            from semarchy_repository.mta_integ_batch
            having max(upddate) is null
                or max(upddate) < now() - (%d * interval '1 minute');
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

            log.info("JOB_BLOCKED_MINUTES=" + JOB_BLOCKED_MINUTES);
            log.info("blockedJobs SQL = " + sqlBlockedJobs(JOB_BLOCKED_MINUTES)); 
            log.info("blockedJobs size = " + (blockedJobs == null ? -1 : blockedJobs.size()));
            log.info("blockedJobs content = " + blockedJobs);

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
            String subject = "[ALERT] MDM Account Monitoring - " + envName + " - " + now;

            String html = buildHtml(envName, now, blockedJobs, notifErrors, engineStopped);

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

        sb.append("<html><body style='font-family:Arial, sans-serif;font-size:13px;'>");

        sb.append("<h2 style='color:#a60000;'>")
          .append("🔴 [ALERT] MDM Account Monitoring")
          .append("</h2>");

        sb.append("<p>")
                .append("<b>Env:</b> ").append(escape(env)).append("<br/>")
                .append("<b>Date:</b> ").append(escape(now)).append("<br/>")
                .append("<b>Fréquence:</b> toutes les 5 minutes")
                .append("</p>");

        if (hasRows(blockedJobs)) {
            sb.append("<h3>🟥 Jobs – SUSPENDED / FAILED</h3>");
            sb.append(actionBlock("""
                Actions recommandées :
                1. Aller dans Semarchy > Application Builder > Management > Moteur d’exécution.
                2. Redémarrer le job depuis le moteur d’exécution.
                3. Analyser ensuite l’erreur dans la colonne job_message.
                """));
            sb.append(toAlertHtmlTable(blockedJobs));
        }

        if (hasRows(notifErrors)) {
            sb.append("<h3>🟥 Data Notifications – Erreurs / Suspensions</h3>");
            sb.append(actionBlock("""
                Actions recommandées :
                1. Aller dans Semarchy > Application Builder > Management > Notifications des données.
                2. Double-cliquer sur la notification en erreur.
                3. Déplier la notification suspendue.
                4. Afficher les logs dans les derniers journaux.
                5. Analyser l’erreur de suspension.
                6. Cliquer sur le bouton d’action puis sélectionner : traiter ces instances de notifications en un lot.
                """));
            sb.append(toAlertHtmlTable(notifErrors));
        }

        if (hasRows(engineStopped)) {
            sb.append("<h3>🟥 Semarchy Engine probablement arrêté</h3>");
            sb.append(actionBlock("""
                Actions recommandées :
                1. Aller dans Semarchy > Application Builder > Management > Moteur d’exécution.
                2. Vérifier l’état du moteur.
                3. Appuyer sur le bouton Play vert pour redémarrer le moteur d’exécution.
                4. Relancer ensuite le check ou attendre la prochaine exécution automatique.
                """));
            sb.append("<p style='color:#a60000;'>")
                    .append("Aucun batch récent détecté. Dernier batch exécuté il y a plus de ")
                    .append(ENGINE_IDLE_MINUTES)
                    .append(" minutes.")
                    .append("</p>");
            sb.append(toAlertHtmlTable(engineStopped));
        }

        sb.append("<hr/>")
                .append("<p style='color:#666;font-size:12px;'>")
                .append("Generated by Azure Function Java - MDM Account Monitoring")
                .append("</p>");

        sb.append("</body></html>");

        return sb.toString();
    }

    private static String actionBlock(String text) {
        return "<div style='background:#fff4e5;border-left:4px solid #f59f00;"
                + "padding:10px;margin:8px 0 12px 0;font-size:13px;line-height:1.45;'>"
                + "<pre style='font-family:Arial, sans-serif;white-space:pre-wrap;margin:0;'>"
                + escape(text.trim())
                + "</pre></div>";
    }

    private static String toAlertHtmlTable(List<Map<String, Object>> rows) {
        if (rows == null || rows.isEmpty()) {
            return "<p style='color:#2e7d32;'>Aucun élément</p>";
        }

        StringBuilder sb = new StringBuilder();

        sb.append("<table style='border-collapse:collapse;width:100%;font-family:Arial,sans-serif;font-size:12px;table-layout:fixed;'>");

        sb.append("<tr style='background:#f2f2f2;'>");
        for (String col : rows.get(0).keySet()) {
            sb.append("<th style='border:1px solid #ccc;padding:6px;text-align:left;'>")
                    .append(escape(col))
                    .append("</th>");
        }
        sb.append("</tr>");

        for (Map<String, Object> row : rows) {
            sb.append("<tr>");

            for (Map.Entry<String, Object> cell : row.entrySet()) {
                String col = cell.getKey();
                String value = cell.getValue() == null ? "" : String.valueOf(cell.getValue());

                String style = "border:1px solid #ccc;padding:6px;vertical-align:top;"
                        + "white-space:pre-wrap;word-break:break-word;overflow-wrap:anywhere;";

                if ("job_message".equalsIgnoreCase(col) || "error_message".equalsIgnoreCase(col)) {
                    style += "max-width:260px;width:260px;";
                    value = wrapText(value, 55);
                }

                sb.append("<td style='").append(style).append("'>")
                        .append(escape(value))
                        .append("</td>");
            }

            sb.append("</tr>");
        }

        sb.append("</table>");
        return sb.toString();
    }

    private static String wrapText(String text, int maxLineLength) {
        if (text == null || text.isBlank()) {
            return "";
        }

        StringBuilder sb = new StringBuilder();
        int count = 0;

        for (String word : text.split("\\s+")) {
            if (count + word.length() > maxLineLength) {
                sb.append("\n");
                count = 0;
            }
            sb.append(word).append(" ");
            count += word.length() + 1;
        }

        return sb.toString().trim();
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

    private static String escape(String s) {
        if (s == null) return "";
        return s.replace("&", "&amp;")
                .replace("<", "&lt;")
                .replace(">", "&gt;")
                .replace("\"", "&quot;");
    }
}
