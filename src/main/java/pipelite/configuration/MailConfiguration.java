/*
 * Copyright 2020 EMBL - European Bioinformatics Institute
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package pipelite.configuration;

import java.util.Properties;
import javax.annotation.PostConstruct;
import lombok.Data;
import lombok.extern.flogger.Flogger;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.mail.javamail.JavaMailSenderImpl;

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
      log.atWarning().log("Missing optional pipelite property: ppipelite.mail.port");
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
  private boolean processFailed = true;
  private boolean processCompleted;
  private boolean stageError = true;
  private boolean stageSuccess;

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
