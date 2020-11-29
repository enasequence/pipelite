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

import static pipelite.executor.ConfigurableStageExecutorParameters.DEFAULT_IMMEDIATE_RETRIES;
import static pipelite.executor.ConfigurableStageExecutorParameters.DEFAULT_MAX_RETRIES;
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
import pipelite.configuration.*;
import pipelite.entity.StageEntity;
import pipelite.entity.StageOutEntity;
import pipelite.executor.StageExecutor;
import pipelite.executor.StageExecutorParameters;
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

@Flogger
public class ProcessLauncher {

  private final LauncherConfiguration launcherConfiguration;
  private final StageConfiguration stageConfiguration;
  private final ProcessService processService;
  private final StageService stageService;
  private final MailService mailService;
  private final String pipelineName;
  private final Process process;

  private final Set<String> activeStages = ConcurrentHashMap.newKeySet();
  private final Duration stageLaunchFrequency;
  private final Duration stagePollFrequency;

  private final AtomicLong stageFailedCount = new AtomicLong(0);
  private final AtomicLong stageSuccessCount = new AtomicLong(0);

  private final ExecutorService executorService;

  private final LocalDateTime startTime = LocalDateTime.now();

  public ProcessLauncher(
      LauncherConfiguration launcherConfiguration,
      StageConfiguration stageConfiguration,
      ProcessService processService,
      StageService stageService,
      MailService mailService,
      String pipelineName,
      Process process) {

    this.launcherConfiguration = launcherConfiguration;
    this.stageConfiguration = stageConfiguration;
    this.processService = processService;
    this.stageService = stageService;
    this.mailService = mailService;
    this.pipelineName = pipelineName;
    this.process = process;

    if (process == null) {
      throw new IllegalArgumentException("Missing process");
    }
    if (process.getProcessEntity() == null) {
      throw new IllegalArgumentException("Missing process entity");
    }

    if (launcherConfiguration.getStageLaunchFrequency() != null) {
      this.stageLaunchFrequency = launcherConfiguration.getStageLaunchFrequency();
    } else {
      this.stageLaunchFrequency = LauncherConfiguration.DEFAULT_STAGE_LAUNCH_FREQUENCY;
    }

    if (launcherConfiguration.getStagePollFrequency() != null) {
      this.stagePollFrequency = launcherConfiguration.getStagePollFrequency();
    } else {
      this.stagePollFrequency = LauncherConfiguration.DEFAULT_STAGE_POLL_FREQUENCY;
    }

    this.executorService = Executors.newCachedThreadPool();
  }

  public static int getMaximumRetries(Stage stage) {
    int maximumRetries = DEFAULT_MAX_RETRIES;
    if (stage.getExecutorParams().getMaximumRetries() != null) {
      maximumRetries = stage.getExecutorParams().getMaximumRetries();
    }
    return Math.max(0, maximumRetries);
  }

  public static int getImmediateRetries(Stage stage) {
    int immediateRetries = DEFAULT_IMMEDIATE_RETRIES;
    if (stage.getExecutorParams().getImmediateRetries() != null) {
      immediateRetries = stage.getExecutorParams().getImmediateRetries();
    }
    return Math.min(Math.max(0, immediateRetries), getMaximumRetries(stage));
  }

  // TODO: orphaned saved stages
  public ProcessState run() {
    logContext(log.atInfo()).log("Running process launcher");
    processService.startExecution(process.getProcessEntity());
    for (Stage stage : process.getStages()) {
      beforeExecution(stage);
    }
    while (true) {
      logContext(log.atFine()).log("Executing stages");
      List<Stage> executableStages = DependencyResolver.getExecutableStages(process.getStages());
      if (activeStages.isEmpty() && executableStages.isEmpty()) {
        break;
      }
      for (Stage stage : executableStages) {
        boolean isActive = activeStages.contains(stage.getStageName());
        if (isActive) {
          // Do not execute this stage because it is already active.
          continue;
        }
        if (stage.getDependsOn() != null) {
          for (Stage dependsOn :
              DependencyResolver.getDependsOnStages(process.getStages(), stage)) {
            if (activeStages.contains(dependsOn.getStageName())) {
              isActive = true;
              break;
            }
          }
        }
        if (isActive) {
          // Do not execute this stage because a stage it depends on is active.
          continue;
        }
        activeStages.add(stage.getStageName());
        executorService.execute(
            () -> {
              try {
                executeStage(stage);
              } catch (Exception ex) {
                logContext(log.atSevere())
                    .withCause(ex)
                    .log("Unexpected exception when executing stage");
              } finally {
                activeStages.remove(stage.getStageName());
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

    return endExecution();
  }

  private void executeStage(Stage stage) {
    String stageName = stage.getStageName();
    logContext(log.atInfo(), stageName).log("Executing stage");

    StageExecutionResult result;
    try {
      startExecution(stage);
      result = stage.getExecutor().execute(pipelineName, getProcessId(), stage);
      if (result.isActive()) {
        // If the execution state is active then the executor is asynchronous.
        result = pollExecution(stage);
      }
    } catch (Exception ex) {
      result = StageExecutionResult.error();
      result.addExceptionAttribute(ex);
    }

    endExecution(stage, result);
  }

  private void beforeExecution(Stage stage) {
    // Add default executor parameters.
    stage.getExecutorParams().add(stageConfiguration);
    StageEntity stageEntity =
        stageService.beforeExecution(pipelineName, process.getProcessId(), stage).get();
    stage.setStageEntity(stageEntity);
  }

  // TODO: test deserialization of active executor
  private void startExecution(Stage stage) {
    StageEntity stageEntity = stage.getStageEntity();
    // Attempt to deserialize stage executor to allow an asynchronous executor to continue executing
    // an active stage.
    if (stageEntity.getResultType() == ACTIVE
        && stageEntity.getExecutorName() != null
        && stageEntity.getExecutorData() != null
        && stageEntity.getExecutorParams() != null) {
      StageExecutor deserializedExecutor = deserializeExecutor(stage);
      StageExecutorParameters deserializedExecutorParams = deserializeExecutorParameters(stage);
      if (deserializedExecutor != null && deserializedExecutorParams != null) {
        logContext(log.atInfo(), stage.getStageName()).log("Using deserialized executor");
        stage.setExecutor(deserializedExecutor);
        stage.setExecutorParams(deserializedExecutorParams);
        return;
      }
    }
    stageService.startExecution(stage);
  }

  private StageExecutionResult pollExecution(Stage stage) {
    StageExecutionResult result;
    while (true) {
      try {
        logContext(log.atInfo(), stage.getStageName())
            .log("Waiting asynchronous stage execution to complete");
        result = stage.getExecutor().execute(pipelineName, getProcessId(), stage);
        if (!result.isActive()) {
          // The asynchronous stage execution has completed.
          break;
        }
      } catch (Exception ex) {
        result = StageExecutionResult.error();
        result.addExceptionAttribute(ex);
        break;
      }
      try {
        Thread.sleep(stagePollFrequency.toMillis());
      } catch (InterruptedException ex) {
        Thread.currentThread().interrupt();
        break;
      }
    }
    return result;
  }

  private void endExecution(Stage stage, StageExecutionResult result) {
    stage.incrementImmediateExecutionCount();
    StageEntity stageEntity = stage.getStageEntity();
    stageService.endExecution(stage, result);
    if (result.isSuccess()) {
      stageSuccessCount.incrementAndGet();
      logContext(log.atInfo(), stageEntity.getStageName())
          .with(LogKey.STAGE_EXECUTION_RESULT_TYPE, stageEntity.getResultType())
          .with(LogKey.STAGE_EXECUTION_COUNT, stageEntity.getExecutionCount())
          .log("Stage executed successfully.");
      invalidateDependentStages(stage);
    } else if (result.isError()) {
      stageFailedCount.incrementAndGet();
      logContext(log.atSevere(), stageEntity.getStageName())
          .with(LogKey.STAGE_EXECUTION_RESULT_TYPE, stageEntity.getResultType())
          .with(LogKey.STAGE_EXECUTION_COUNT, stageEntity.getExecutionCount())
          .log("Stage execution failed");
      // Send email after failed stage execution.
      mailService.sendStageExecutionMessage(pipelineName, process, stage);
    } else {
      throw new RuntimeException(
          "Unexpected stage execution result type: " + result.getResultType().name());
    }
  }

  private ProcessState endExecution() {
    ProcessState processState = evaluateProcessState(process.getStages());
    logContext(log.atInfo()).log("Process execution finished: %s", processState.name());
    processService.endExecution(pipelineName, process, processState);
    return processState;
  }

  private StageExecutor deserializeExecutor(Stage stage) {
    StageEntity stageEntity = stage.getStageEntity();
    String stageName = stage.getStageName();
    try {
      return StageExecutor.deserialize(
          stageEntity.getExecutorName(), stageEntity.getExecutorData());
    } catch (Exception ex) {
      logContext(log.atSevere(), stageName)
          .withCause(ex)
          .log("Failed to deserialize executor: %s", stageEntity.getExecutorName());
    }
    return null;
  }

  private StageExecutorParameters deserializeExecutorParameters(Stage stage) {
    StageEntity stageEntity = stage.getStageEntity();
    String stageName = stage.getStageName();
    try {
      return StageExecutorParameters.deserialize(stageEntity.getExecutorParams());
    } catch (Exception ex) {
      logContext(log.atSevere(), stageName)
          .withCause(ex)
          .log("Failed to deserialize executor parameters: %s", stageEntity.getExecutorName());
    }
    return null;
  }

  public static ProcessState evaluateProcessState(List<Stage> stages) {
    int errorCount = 0;
    for (Stage stage : stages) {
      StageEntity stageEntity = stage.getStageEntity();
      StageExecutionResultType resultType = stageEntity.getResultType();
      if (resultType == SUCCESS) {
        continue;
      }
      if (DependencyResolver.isExecutableStage(stages, stage)) {
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

  private void invalidateDependentStages(Stage from) {
    for (Stage stage : DependencyResolver.getDependentStages(process.getStages(), from)) {
      StageEntity stageEntity = stage.getStageEntity();
      stageEntity.resetExecution();
      stageService.saveStage(stageEntity);
      stageService.saveStageOut(StageOutEntity.resetExecution(stageEntity));
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

  private FluentLogger.Api logContext(FluentLogger.Api log, String stageName) {
    return log.with(LogKey.PIPELINE_NAME, pipelineName)
        .with(LogKey.PROCESS_ID, getProcessId())
        .with(LogKey.STAGE_NAME, stageName);
  }
}
