package pipelite.configuration;

import lombok.Data;
import lombok.extern.flogger.Flogger;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.mail.javamail.JavaMailSenderImpl;

import javax.annotation.PostConstruct;
import java.util.Properties;

@Configuration
@ConfigurationProperties(prefix = "pipelite.mail")
@Data
@Flogger
public class MailConfiguration {

  @PostConstruct
  private void checkOptionalProperties() {
    if (host == null) {
      log.atWarning().log("Missing optional pipelite property: pipelite.mail.host");
    }
    if (port == null) {
      log.atWarning().log("Missing optional pipelite property: ppipelite.mail.host");
    }
    if (from == null) {
      log.atWarning().log("Missing optional pipelite property: pipelite.mail.from");
    }
    if (to == null) {
      log.atWarning().log("Missing optional pipelite property: pipelite.mail.to");
    }
    if (starttls == null) {
      log.atWarning().log("Missing optional pipelite property: pipelite.mail.starttls");
    }
    if (username == null) {
      log.atWarning().log("Missing optional pipelite property: pipelite.mail.username");
    }
    if (password == null) {
      log.atWarning().log("Missing optional pipelite property: pipelite.mail.password");
    }
  }

  private String host;
  private Integer port;
  private String from;
  private String to;
  private Boolean starttls;
  private String username;
  private String password;

  @Bean
  @ConditionalOnProperty({
    "pipelite.mail.host",
    "pipelite.mail.port",
    "pipelite.mail.from",
    "pipelite.mail.to"
  })
  public JavaMailSender mailSender() {
    JavaMailSenderImpl mailSender = new JavaMailSenderImpl();
    mailSender.setHost(host);
    mailSender.setPort(port);
    if (username != null) {
      mailSender.setUsername(username);
    }
    if (password != null) {
      mailSender.setPassword(password);
    }
    Properties props = mailSender.getJavaMailProperties();
    props.put("mail.transport.protocol", "smtp");
    // props.put("mail.smtp.auth", "true");
    if (starttls != null && starttls) {
      props.put("mail.smtp.starttls.enable", "true");
    }
    // props.put("mail.debug", "true");
    return mailSender;
  }
}
