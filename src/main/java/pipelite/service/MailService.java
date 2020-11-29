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
package pipelite.service;

import lombok.extern.flogger.Flogger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.mail.SimpleMailMessage;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.stereotype.Component;
import pipelite.configuration.MailConfiguration;
import pipelite.process.Process;
import pipelite.stage.Stage;

@Component
@Flogger
public class MailService {

  @Autowired private MailConfiguration mailConfiguration;

  @Autowired(required = false)
  private JavaMailSender mailSender;

  public void sendProcessExecutionMessage(String pipelineName, Process process) {
    if (mailSender == null) {
      return;
    }
    SimpleMailMessage message = new SimpleMailMessage();
    message.setFrom(mailConfiguration.getFrom());
    message.setTo(mailConfiguration.getTo());
    String subject = getProcessExecutionSubject(pipelineName, process);
    message.setSubject(subject);
    String text = getExecutionBody(process, subject);
    message.setText(text);
    mailSender.send(message);
  }

  public void sendStageExecutionMessage(String pipelineName, Process process, Stage stage) {
    if (mailSender == null) {
      return;
    }
    SimpleMailMessage message = new SimpleMailMessage();
    message.setFrom(mailConfiguration.getFrom());
    message.setTo(mailConfiguration.getTo());
    String subject = getStageExecutionSubject(pipelineName, process, stage);
    message.setSubject(subject);
    String text = getExecutionBody(process, subject);
    message.setText(text);
    mailSender.send(message);
  }

  public String getProcessExecutionSubject(String pipelineName, Process process) {
    String state = "";
    if (process.getProcessEntity().getState() != null) {
      state = process.getProcessEntity().getState().name();
    }
    return "pipelite process (" + state + "): " + pipelineName + "/" + process.getProcessId();
  }

  public String getStageExecutionSubject(String pipelineName, Process process, Stage stage) {
    String state = "";
    if (stage.getStageEntity() != null && stage.getStageEntity().getResultType() != null) {
      state = stage.getStageEntity().getResultType().name();
    } else {
      state = "PENDING";
    }
    return "pipelite stage ("
        + state
        + "): "
        + pipelineName
        + "/"
        + process.getProcessId()
        + "/"
        + stage.getStageName();
  }

  public String getExecutionBody(Process process, String subject) {
    return subject + "\n" + getProcessText(process) + getStagesText(process);
  }

  public String getProcessText(Process process) {
    if (process.getProcessEntity() == null) {
      return "";
    }
    return "\nProcess:\n---------------\n" + process.getProcessEntity().serialize() + "\n";
  }

  public String getStagesText(Process process) {
    String text = "";
    if (!process.getStages().isEmpty()) {
      text += "\nStages:\n---------------\n";
    }
    for (Stage stage : process.getStages()) {
      if (stage.getStageEntity() != null) {
        text += stage.getStageEntity().serialize() + "\n";
      }
    }
    return text;
  }
}
