package com.cdh.monitoring;

import jakarta.mail.*;
import jakarta.mail.internet.InternetAddress;
import jakarta.mail.internet.MimeMessage;
import jakarta.mail.internet.MimeUtility;

import java.nio.charset.StandardCharsets;
import java.util.Properties;

public class MailUtil {

    public static void sendHtml(
            String smtpHost,
            int smtpPort,
            String smtpUser,
            String smtpPass,
            String mailFrom,
            String mailTo,
            String subject,
            String html
    ) throws Exception {

        Properties props = new Properties();
        props.put("mail.smtp.host", smtpHost);
        props.put("mail.smtp.port", String.valueOf(smtpPort));
        props.put("mail.smtp.auth", "false");
        props.put("mail.smtp.starttls.enable", "true");

        // Optionnel mais utile en prod
        props.put("mail.smtp.connectiontimeout", "15000");
        props.put("mail.smtp.timeout", "30000");
        props.put("mail.smtp.writetimeout", "30000");

        Session session = Session.getInstance(props, new Authenticator() {
            @Override
            protected PasswordAuthentication getPasswordAuthentication() {
                return new PasswordAuthentication(smtpUser, smtpPass);
            }
        });

        MimeMessage message = new MimeMessage(session);
        message.setFrom(new InternetAddress(mailFrom));
        message.setRecipients(Message.RecipientType.TO, InternetAddress.parse(mailTo));

        // ✅ Subject UTF-8 compatible Jakarta Mail
        message.setSubject(MimeUtility.encodeText(subject, StandardCharsets.UTF_8.name(), "Q"));

        // ✅ HTML UTF-8
        message.setContent(html, "text/html; charset=UTF-8");

        Transport.send(message);
    }
}
