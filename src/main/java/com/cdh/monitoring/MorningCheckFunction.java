package com.cdh.monitoring;

import com.microsoft.azure.functions.ExecutionContext;
import com.microsoft.azure.functions.annotation.FunctionName;
import com.microsoft.azure.functions.annotation.TimerTrigger;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;

public class MorningCheckFunction {

    // ✅ Planification : tous les jours à 07:30 (UTC par défaut)
    // Pour tester vite : "0 */5 * * * *" (toutes les 5 min) puis tu remets 07:30
    // 0 30 7 * * *
    private static final String CRON = "0 */5 * * * *";

    // --- SQL (adapte si besoin) ---
    private static final String SQL_STATUS_COUNTS = """
        select status, count(*) as nb
        from semarchy_repository.mta_integ_batch
        where upddate >= now() - interval '24 hours'
        group by status
        order by nb desc;
    """;

    private static final String SQL_FAILED_DETAILS = """
        select
          mdl."name" as datalocation,
          mib.upddate,
          mib.status,
          mib.job_name,
          left(coalesce(mib.job_message,''), 300) as job_message,
          mib.batchid
        from semarchy_repository.mta_integ_batch mib
        join semarchy_repository.mta_data_location mdl
          on mdl."uuid" = mib.o_datalocation
        where mib.upddate >= now() - interval '24 hours'
          and mib.status in ('FAILED','ERROR','SUSPENDED')
        order by mib.upddate desc;
    """;

    private static final String SQL_STUCK = """
        select job_name, status, upddate
        from semarchy_repository.mta_integ_batch
        where status in ('RUNNING','PENDING')
          and upddate < now() - interval '60 minutes'
        order by upddate asc;
    """;

    @FunctionName("MorningCheckSemarchy")
    public void run(
        @TimerTrigger(name = "timerInfo", schedule = CRON) String timerInfo,
        final ExecutionContext context
    ) {
        var log = context.getLogger();
        log.info("MorningCheck START - timerInfo=" + timerInfo);

        try {
            // 1) Lire la config
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

            String jdbcUrl = "jdbc:postgresql://" + dbHost + ":" + dbPort + "/" + dbName;

            // 2) Exécuter KPI
            List<Map<String, Object>> statusCounts = DbUtil.query(jdbcUrl, dbUser, dbPass, SQL_STATUS_COUNTS);
            List<Map<String, Object>> failedDetails = DbUtil.query(jdbcUrl, dbUser, dbPass, SQL_FAILED_DETAILS);
            List<Map<String, Object>> stuckJobs = DbUtil.query(jdbcUrl, dbUser, dbPass, SQL_STUCK);

            boolean hasRed = (failedDetails != null && !failedDetails.isEmpty());
            String emoji = hasRed ? "🔴" : "🟢";

            // 3) Construire HTML
            String now = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm"));
            String html = buildHtml(envName, now, statusCounts, failedDetails, stuckJobs);

            // 4) Envoyer mail
            String subject = emoji + " Morning Check Semarchy xDM – " + envName + " – " + now;
            MailUtil.sendHtml(smtpHost, smtpPort, smtpUser, smtpPass, mailFrom, mailTo, subject, html);

            log.info("MorningCheck OK - email sent. failed=" + failedDetails.size() + " stuck=" + stuckJobs.size());

        } catch (Exception e) {
            log.severe("MorningCheck FAILED: " + e.getMessage());
            // log full stack
            e.printStackTrace();
        } finally {
            log.info("MorningCheck END");
        }
    }

    private static String buildHtml(String env, String now,
                                    List<Map<String, Object>> statusCounts,
                                    List<Map<String, Object>> failedDetails,
                                    List<Map<String, Object>> stuckJobs) {

        String statusTable = DbUtil.toHtmlTable(statusCounts);
        String failedTable = DbUtil.toHtmlTable(failedDetails);
        String stuckTable = DbUtil.toHtmlTable(stuckJobs);

        String failedBlock = (failedDetails == null || failedDetails.isEmpty())
            ? "<p>✅ Aucune anomalie détectée</p>"
            : failedTable;

        String stuckBlock = (stuckJobs == null || stuckJobs.isEmpty())
            ? "<p>✅ Aucun job bloqué détecté</p>"
            : stuckTable;

        return """
            <html>
              <body style="font-family:Arial, sans-serif;">
                <h2>📅 MORNING CHECK – Semarchy xDM</h2>
                <p><b>Env:</b> %s<br/>
                   <b>Date:</b> %s</p>

                <h3> Jobs – Compteurs (24h)</h3>
                %s

                <h3> Anomalies (FAILED/ERROR/SUSPENDED)</h3>
                %s

                <h3> Jobs potentiellement bloqués (&gt;60min)</h3>
                %s

                <hr/>
                <p style="color:#666;font-size:12px;">
                  Generated by Azure Function (Java)
                </p>
              </body>
            </html>
        """.formatted(env, now, statusTable, failedBlock, stuckBlock);
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
}
