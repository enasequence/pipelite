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
import pipelite.entity.ProcessEntity;
import pipelite.process.Process;
import pipelite.process.ProcessState;
import pipelite.stage.Stage;
import pipelite.stage.StageState;

@Component
@Flogger
public class MailService {

  @Autowired private MailConfiguration mailConfiguration;

  @Autowired(required = false)
  private JavaMailSender mailSender;

  public void sendProcessExecutionMessage(Process process) {
    if (mailSender == null) {
      return;
    }
    if ((mailConfiguration.isProcessCompleted()
            && process.getProcessEntity().getProcessState() == ProcessState.COMPLETED)
        || (mailConfiguration.isProcessFailed()
            && process.getProcessEntity().getProcessState() == ProcessState.FAILED)) {
      SimpleMailMessage message = new SimpleMailMessage();
      message.setFrom(mailConfiguration.getFrom());
      message.setTo(mailConfiguration.getTo());
      String subject = getProcessExecutionSubject(process);
      message.setSubject(subject);
      String text = getExecutionBody(process, subject);
      message.setText(text);
      mailSender.send(message);
    }
  }

  public void sendStageExecutionMessage(Process process, Stage stage) {
    if (mailSender == null) {
      return;
    }
    if ((mailConfiguration.isStageSuccess()
            && stage.getStageEntity().getStageState() == StageState.SUCCESS)
        || (mailConfiguration.isStageError()
            && stage.getStageEntity().getStageState() == StageState.ERROR)) {
      SimpleMailMessage message = new SimpleMailMessage();
      message.setFrom(mailConfiguration.getFrom());
      message.setTo(mailConfiguration.getTo());
      String subject = getStageExecutionSubject(process, stage);
      message.setSubject(subject);
      String text = getExecutionBody(process, subject);
      message.setText(text);
      mailSender.send(message);
    }
  }

  public String getProcessExecutionSubject(Process process) {
    ProcessEntity processEntity = process.getProcessEntity();
    String state = "";
    if (processEntity.getProcessState() != null) {
      state = processEntity.getProcessState().name();
    }
    return "Pipelite process ("
        + state
        + "): "
        + processEntity.getPipelineName()
        + "/"
        + process.getProcessId();
  }

  public String getStageExecutionSubject(Process process, Stage stage) {
    String state = "";
    if (stage.getStageEntity() != null) {
      state = stage.getStageEntity().getStageState().name();
    }
    return "Pipelite stage ("
        + state
        + "): "
        + process.getProcessEntity().getPipelineName()
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
