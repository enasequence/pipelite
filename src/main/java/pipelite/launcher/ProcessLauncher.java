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
package pipelite.launcher;

import static pipelite.stage.StageExecutionResultType.*;

import com.google.common.flogger.FluentLogger;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import lombok.extern.flogger.Flogger;
import org.springframework.util.Assert;
import pipelite.configuration.*;
import pipelite.entity.StageEntity;
import pipelite.executor.StageExecutorSerializer;
import pipelite.launcher.dependency.DependencyResolver;
import pipelite.log.LogKey;
import pipelite.process.Process;
import pipelite.process.ProcessState;
import pipelite.service.MailService;
import pipelite.service.ProcessService;
import pipelite.service.StageService;
import pipelite.stage.Stage;
import pipelite.stage.StageExecutionResult;
import pipelite.stage.StageExecutionResultType;

/** Executes a process and returns the process state. */
@Flogger
public class ProcessLauncher {

  private final LauncherConfiguration launcherConfiguration;
  private final StageConfiguration stageConfiguration;
  private final ProcessService processService;
  private final StageService stageService;
  private final MailService mailService;
  private final Duration stageLaunchFrequency;
  private final Set<Stage> active = ConcurrentHashMap.newKeySet();
  private final AtomicLong stageFailedCount = new AtomicLong(0);
  private final AtomicLong stageSuccessCount = new AtomicLong(0);
  private final ExecutorService executorService = Executors.newCachedThreadPool();
  private final LocalDateTime startTime = LocalDateTime.now();
  private String pipelineName;
  private Process process;

  public ProcessLauncher(
      LauncherConfiguration launcherConfiguration,
      StageConfiguration stageConfiguration,
      ProcessService processService,
      StageService stageService,
      MailService mailService) {
    Assert.notNull(launcherConfiguration, "Missing launcher configuration");
    Assert.notNull(stageConfiguration, "Missing stage configuration");
    Assert.notNull(processService, "Missing process service");
    Assert.notNull(stageService, "Missing stage service");
    Assert.notNull(mailService, "Missing mail service");
    this.launcherConfiguration = launcherConfiguration;
    this.stageConfiguration = stageConfiguration;
    this.processService = processService;
    this.stageService = stageService;
    this.mailService = mailService;
    this.stageLaunchFrequency = launcherConfiguration.getStageLaunchFrequency();
  }

  // TODO: orphaned saved stages
  /** Executes the process and sets the new process state. */
  public void run(String pipelineName, Process process) {
    Assert.notNull(pipelineName, "Missing pipeline name");
    Assert.notNull(process, "Missing process");
    Assert.notNull(process.getProcessEntity(), "Missing process entity");
    this.pipelineName = pipelineName;
    this.process = process;
    logContext(log.atInfo()).log("Executing process");
    beforeExecution();
    processService.startExecution(process.getProcessEntity());
    while (true) {
      logContext(log.atFine()).log("Executing stages");
      List<Stage> executableStages =
          DependencyResolver.getImmediatelyExecutableStages(process.getStages(), active);
      if (active.isEmpty() && executableStages.isEmpty()) {
        logContext(log.atInfo()).log("No more executable stages");
        break;
      }
      for (Stage stage : executableStages) {
        StageLauncher stageLauncher =
            new StageLauncher(
                launcherConfiguration, stageConfiguration, pipelineName, process, stage);
        active.add(stage);
        executorService.execute(
            () -> {
              try {
                if (!StageExecutorSerializer.deserializeExecution(stage)) {
                  stageService.startExecution(stage);
                }
                StageExecutionResult result = stageLauncher.run();
                stageService.endExecution(stage, result);
                if (result.isSuccess()) {
                  resetDependentStageExecution(stage);
                  stageSuccessCount.incrementAndGet();
                } else {
                  mailService.sendStageExecutionMessage(pipelineName, process, stage);
                  stageFailedCount.incrementAndGet();
                }
              } catch (Exception ex) {
                stageService.endExecution(stage, StageExecutionResult.error(ex));
                mailService.sendStageExecutionMessage(pipelineName, process, stage);
                logContext(log.atSevere())
                    .withCause(ex)
                    .log("Unexpected exception when executing stage");
              } finally {
                active.remove(stage);
              }
            });
      }
      try {
        Thread.sleep(stageLaunchFrequency.toMillis());
      } catch (InterruptedException ex) {
        logContext(log.atSevere()).log("Process launcher was interrupted");
        executorService.shutdownNow();
        Thread.currentThread().interrupt();
        throw new RuntimeException(ex);
      }
    }
    endExecution();
  }

  private void beforeExecution() {
    for (Stage stage : process.getStages()) {
      // Set default executor parameters.
      stage.getExecutorParams().add(stageConfiguration);
      // Set stage entity.
      StageEntity stageEntity =
          stageService.beforeExecution(pipelineName, process.getProcessId(), stage).get();
      stage.setStageEntity(stageEntity);
    }
  }

  private void endExecution() {
    ProcessState processState = evaluateProcessState(process.getStages());
    logContext(log.atInfo()).log("Process execution finished: %s", processState.name());
    processService.endExecution(pipelineName, process, processState);
  }

  /**
   * Evaluates the process state using the stage execution result types.
   *
   * @param stages list of stages
   * @return the process state
   */
  public static ProcessState evaluateProcessState(List<Stage> stages) {
    int errorCount = 0;
    for (Stage stage : stages) {
      StageEntity stageEntity = stage.getStageEntity();
      StageExecutionResultType resultType = stageEntity.getResultType();
      if (resultType == SUCCESS) {
        continue;
      }
      if (DependencyResolver.isEventuallyExecutableStage(stages, stage)) {
        return ProcessState.ACTIVE;
      } else {
        errorCount++;
      }
    }
    if (errorCount > 0) {
      return ProcessState.FAILED;
    }
    return ProcessState.COMPLETED;
  }

  /**
   * Resets the stage execution of all dependent stages.
   *
   * @param from the stage that has dependent stages
   */
  private void resetDependentStageExecution(Stage from) {
    for (Stage stage : DependencyResolver.getDependentStages(process.getStages(), from)) {
      if (stage.getStageEntity().getResultType() != null) {
        stageService.resetExecution(stage);
      }
    }
  }

  public String getPipelineName() {
    return pipelineName;
  }

  public String getProcessId() {
    return process.getProcessId();
  }

  public LocalDateTime getStartTime() {
    return startTime;
  }

  public long getStageFailedCount() {
    return stageFailedCount.get();
  }

  public long getStageSuccessCount() {
    return stageSuccessCount.get();
  }

  private FluentLogger.Api logContext(FluentLogger.Api log) {
    return log.with(LogKey.PIPELINE_NAME, pipelineName).with(LogKey.PROCESS_ID, getProcessId());
  }
}
