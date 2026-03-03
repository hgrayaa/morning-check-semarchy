package com.cdh.monitoring;

import jakarta.mail.*;
import jakarta.mail.internet.InternetAddress;
import jakarta.mail.internet.MimeMessage;

import java.util.Properties;

public final class MailUtil {

    private MailUtil() {}

    public static void sendHtml(String host, int port, String user, String pass,
                                String from, String toCsv,
                                String subject, String htmlBody) throws MessagingException {

        Properties props = new Properties();
        props.put("mail.smtp.auth", "true");
        props.put("mail.smtp.starttls.enable", "true");
        props.put("mail.smtp.host", host);
        props.put("mail.smtp.port", String.valueOf(port));
        props.put("mail.smtp.connectiontimeout", "15000");
        props.put("mail.smtp.timeout", "20000");
        props.put("mail.smtp.writetimeout", "20000");

        Session session = Session.getInstance(props, new Authenticator() {
            @Override protected PasswordAuthentication getPasswordAuthentication() {
                return new PasswordAuthentication(user, pass);
            }
        });

        Message msg = new MimeMessage(session);
        msg.setFrom(new InternetAddress(from));

        for (String t : toCsv.split(",")) {
            String email = t.trim();
            if (!email.isEmpty()) {
                msg.addRecipient(Message.RecipientType.TO, new InternetAddress(email));
            }
        }

        msg.setSubject(subject);
        msg.setContent(htmlBody, "text/html; charset=UTF-8");

        Transport.send(msg);
    }
}
