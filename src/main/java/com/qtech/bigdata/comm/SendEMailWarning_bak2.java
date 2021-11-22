package com.qtech.bigdata.comm;

import org.apache.hadoop.fs.Path;

import javax.activation.DataHandler;
import javax.activation.DataSource;
import javax.activation.FileDataSource;
import javax.mail.*;
import javax.mail.internet.*;
import java.io.File;
import java.util.List;
import java.util.Properties;

public class SendEMailWarning_bak2 {


    public static boolean sendMail(List<String> recipients, String subject, String content, File attachment) {

        boolean flag = false;
        try {
            final Properties props = System.getProperties();
            props.put("mail.smtp.auth", "true");
            props.put("mail.smtp.host", "123.58.177.49");
            props.put("mail.user", "bigdata.it@qtechglobal.com");
            props.put("mail.password", "qtech2020");
            props.put("mail.smtp.port", "25");
            props.put("mail.smtp.starttls.enable", "true");
            props.put("mail.smtp.ssl.trust","123.58.177.49");
            // 设置邮箱认证
            Authenticator authenticator = new Authenticator() {
                protected PasswordAuthentication getPasswordAuthentication() {
                    String userName = props.getProperty("mail.user");
                    String password = props.getProperty("mail.password");
                    return new PasswordAuthentication(userName, password);
                }
            };
            // 使用环境属性和授权信息，创建邮件会话
            Session session = Session.getInstance(props, authenticator);
            // 创建邮件消息
            MimeMessage message = new MimeMessage(session);
            // 设置发件人
            InternetAddress fromAddress = new InternetAddress(props.getProperty("mail.user"));
            message.setFrom(fromAddress);


            final int num = recipients.size();
            InternetAddress[] addresses = new InternetAddress[num];
            for (int i = 0; i < num; i++) {
                addresses[i] = new InternetAddress(recipients.get(i));
            }
            message.setRecipients(MimeMessage.RecipientType.TO, addresses);


            Multipart multipart = new MimeMultipart();
            BodyPart contentPart = new MimeBodyPart();

            contentPart.setContent(content, "text/html;charset=UTF-8");

            multipart.addBodyPart(contentPart);


            //设置附件
            if (attachment != null) {

                BodyPart attachmentBodyPart = new MimeBodyPart();

                DataSource source = new FileDataSource(attachment);

                attachmentBodyPart.setDataHandler(new DataHandler(source));

                attachmentBodyPart.setFileName(MimeUtility.encodeWord(attachment.getName()));

                multipart.addBodyPart(attachmentBodyPart);
            }

            message.setContent(multipart);
            // 设置邮件标题
            message.setSubject(subject);
            // 设置邮件的内容体
//            message.setContent(content, "text/html;charset=UTF-8");
            message.saveChanges();
            message.setContent(multipart);
            // 发送邮件
            Transport.send(message);
            flag = true;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return flag;
    }
}
