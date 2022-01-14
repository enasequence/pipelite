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

import java.util.Optional;
import lombok.extern.flogger.Flogger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.mail.SimpleMailMessage;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.stereotype.Component;
import pipelite.configuration.AdvancedConfiguration;
import pipelite.configuration.MailConfiguration;
import pipelite.configuration.ServiceConfiguration;
import pipelite.entity.ProcessEntity;
import pipelite.entity.StageLogEntity;
import pipelite.error.InternalErrorHandler;
import pipelite.executor.task.RetryTask;
import pipelite.process.Process;
import pipelite.process.ProcessState;
import pipelite.stage.Stage;
import pipelite.stage.StageState;

@Component
@Flogger
public class MailService {

  private final AdvancedConfiguration advancedConfiguration;
  private final MailConfiguration mailConfiguration;
  private final StageService stageService;
  private final InternalErrorHandler internalErrorHandler;

  @Autowired(required = false)
  private JavaMailSender mailSender;

  public MailService(
      @Autowired ServiceConfiguration serviceConfiguration,
      @Autowired AdvancedConfiguration advancedConfiguration,
      @Autowired MailConfiguration mailConfiguration,
      @Autowired InternalErrorService internalErrorService,
      @Autowired StageService stageService) {
    this.advancedConfiguration = advancedConfiguration;
    this.mailConfiguration = mailConfiguration;
    this.stageService = stageService;
    this.internalErrorHandler =
        new InternalErrorHandler(internalErrorService, serviceConfiguration.getName(), this);
  }

  /**
   * Sends process execution emails. Saves any internal errors.
   *
   * @param process the process being executed
   */
  public void sendProcessExecutionMessage(Process process) {
    if (mailSender == null) {
      return;
    }
    internalErrorHandler.execute(
        () -> {
          if ((mailConfiguration.isProcessCompleted()
                  && process.getProcessEntity().getProcessState() == ProcessState.COMPLETED)
              || (mailConfiguration.isProcessFailed()
                  && process.getProcessEntity().getProcessState() == ProcessState.FAILED)) {
            SimpleMailMessage message = new SimpleMailMessage();
            message.setFrom(mailConfiguration.getFrom());
            message.setTo(mailConfiguration.getTo().split("\\s*,\\s*"));
            String subject = getProcessExecutionSubject(process);
            message.setSubject(subject);
            String text = getExecutionBody(process, subject);
            message.setText(text);
            RetryTask.DEFAULT.execute(
                r -> {
                  mailSender.send(message);
                  return null;
                });
          }
        });
  }

  /**
   * Sends stage execution emails. Saves any internal errors.
   *
   * @param process the process being executed
   * @param stage the stage being executed
   */
  public void sendStageExecutionMessage(Process process, Stage stage) {
    if (mailSender == null) {
      return;
    }
    internalErrorHandler.execute(
        () -> {
          if ((mailConfiguration.isStageSuccess()
                  && stage.getStageEntity().getStageState() == StageState.SUCCESS)
              || (mailConfiguration.isStageError()
                  && stage.getStageEntity().getStageState() == StageState.ERROR)) {
            SimpleMailMessage message = new SimpleMailMessage();
            message.setFrom(mailConfiguration.getFrom());
            message.setTo(mailConfiguration.getTo().split("\\s*,\\s*"));
            String subject = getStageExecutionSubject(process, stage);
            message.setSubject(subject);
            String text = getExecutionBody(process, subject);
            message.setText(text);
            RetryTask.DEFAULT.execute(
                r -> {
                  mailSender.send(message);
                  return null;
                });
          }
        });
  }

  String getProcessExecutionSubject(Process process) {
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

  String getStageExecutionSubject(Process process, Stage stage) {
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

  String getExecutionBody(Process process, String subject) {
    return subject
        + "\n"
        + getProcessText(process)
        + getStagesText(process)
        + getStagesLogText(process);
  }

  String getProcessText(Process process) {
    if (process.getProcessEntity() == null) {
      return "";
    }
    return "\nProcess:\n---------------\n" + process.getProcessEntity().serialize() + "\n";
  }

  String getStagesText(Process process) {
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

  String getStagesLogText(Process process) {
    if (!process.getStages().stream().anyMatch(s -> s.isError())) {
      return "";
    }
    String text = "\nError logs:\n---------------\n";
    for (Stage stage : process.getStages()) {
      if (stage.isError()) {
        Optional<StageLogEntity> stageLogEntity =
            stageService.getSavedStageLog(
                process.getProcessEntity().getPipelineName(),
                process.getProcessId(),
                stage.getStageName());
        if (stageLogEntity.isPresent()) {
          String log = stageLogEntity.get().getStageLog();
          if (log != null && !log.isEmpty()) {
            text += "\nStage: " + stage.getStageName() + "\n===============\n";
            text +=
                log.substring(Math.max(0, log.length() - advancedConfiguration.getMailLogBytes()))
                    + "\n";
          }
        }
      }
    }
    return text;
  }
}
