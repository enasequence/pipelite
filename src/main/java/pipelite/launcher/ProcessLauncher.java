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
import pipelite.entity.ProcessEntity;
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

  private final List<Stage> stages;
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

    this.stages = new ArrayList<>();

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

  public ProcessState run() {
    logContext(log.atInfo()).log("Running process launcher");
    createStages();
    executeStages();
    return saveProcess();
  }

  // TODO: orphaned saved stages
  private void createStages() {
    List<Stage> stages = process.getStages();

    for (Stage stage : stages) {
      // Adds stage parameter defaults from stage configuration.
      stage.getExecutorParams().add(stageConfiguration);

      // Gets existing stage entity from the database.
      Optional<StageEntity> stageEntity =
          stageService.getSavedStage(pipelineName, process.getProcessId(), stage.getStageName());

      // Saves the stage entity in the database if it does not exist.
      if (!stageEntity.isPresent()) {
        stageEntity =
            Optional.of(
                stageService.saveStage(
                    StageEntity.createExecution(pipelineName, getProcessId(), stage)));
      }

      stage.setStageEntity(stageEntity.get());
      this.stages.add(stage);
    }
  }

  public static ProcessState evaluateProcessState(List<Stage> stages) {
    int successCount = 0;
    int activeCount = 0;
    int errorCount = 0;
    for (Stage stage : stages) {
      StageEntity stageEntity = stage.getStageEntity();
      StageExecutionResultType resultType = stageEntity.getResultType();

      if (resultType == SUCCESS) {
        // The stage execution has been successful.
        successCount++;
      } else if (resultType == null || resultType == NEW || resultType == ACTIVE) {
        // The stage and process execution is active.
        activeCount++;
      } else if (resultType == ERROR) {
        Integer executionCount = stageEntity.getExecutionCount();
        int maximumRetries = getMaximumRetries(stage);
        if (executionCount != null && executionCount > maximumRetries) {
          // The stage and process execution has failed.
          errorCount++;
        } else {
          // The stage and process execution is active.
          activeCount++;
        }
      }
    }

    if (activeCount > 0) {
      if (!DependencyResolver.getExecutableStages(stages).isEmpty()) {
        // All least one stage execution is active so the process is active.
        return ProcessState.ACTIVE;
      }
    }
    if (errorCount > 0) {
      // All least one stage execution has failed so the process has failed.
      return ProcessState.FAILED;
    }

    // All stages and the process have completed successfully.
    return ProcessState.COMPLETED;
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

  private void executeStages() {
    while (true) {
      if (Thread.currentThread().isInterrupted()) {
        executorService.shutdownNow();
        return;
      }

      logContext(log.atFine()).log("Executing stages");

      List<Stage> executableStages = DependencyResolver.getExecutableStages(stages);
      if (activeStages.isEmpty() && executableStages.isEmpty()) {
        logContext(log.atInfo()).log("No active or executable stages");
        return;
      }

      for (Stage stage : executableStages) {
        String stageName = stage.getStageName();
        boolean isActive = activeStages.contains(stageName);
        if (isActive) {
          // We should not execute this stage because it is already active.
          continue;
        }
        if (stage.getDependsOn() != null) {
          for (Stage dependsOn : DependencyResolver.getDependsOnStages(stages, stage)) {
            if (activeStages.contains(dependsOn.getStageName())) {
              isActive = true;
              break;
            }
          }
        }
        if (isActive) {
          // We should not execute this stage because a stage it depends on is active.
          continue;
        }

        activeStages.add(stageName);
        executorService.execute(
            () -> {
              try {
                executeStage(stage);
              } catch (Exception ex) {
                logContext(log.atSevere())
                    .withCause(ex)
                    .log("Unexpected exception when executing stage");
              } finally {
                activeStages.remove(stageName);
              }
            });
      }

      try {
        Thread.sleep(stageLaunchFrequency.toMillis());
      } catch (InterruptedException ex) {
        executorService.shutdownNow();
        Thread.currentThread().interrupt();
        return;
      }
    }
  }

  private ProcessState saveProcess() {
    logContext(log.atInfo()).log("Saving process");
    ProcessEntity processEntity = process.getProcessEntity();
    processEntity.updateExecution(evaluateProcessState(stages));
    processService.saveProcess(processEntity);
    // Send email after process execution.
    mailService.sendProcessExecutionMessage(pipelineName, process);
    return processEntity.getState();
  }

  private void executeStage(Stage stage) {
    StageEntity stageEntity = stage.getStageEntity();
    String stageName = stage.getStageName();
    logContext(log.atInfo(), stageName).log("Preparing to execute stage");

    // If the stage executor has been serialized we should use it to allow an active stage
    // execution to continue. For example, an asynchronous executor may contain a job id that
    // is associated with an external execution service.

    // TODO: test deserialization of active executor
    boolean isUsingDeserializedExecutor = false;
    if (stageEntity.getResultType() == ACTIVE
        && stageEntity.getExecutorName() != null
        && stageEntity.getExecutorData() != null
        && stageEntity.getExecutorParams() != null) {
      StageExecutor deserializedExecutor = deserializeExecutor(stage);
      StageExecutorParameters deserializedExecutorParams = deserializeExecutorParameters(stage);
      boolean isDeserializedExecutor = deserializedExecutor != null;
      boolean isDeserializedExecutorParams = deserializedExecutorParams != null;
      if (isDeserializedExecutor && isDeserializedExecutorParams) {
        // Use deserialized executor.
        isUsingDeserializedExecutor = true;
        stage.setExecutor(deserializedExecutor);
        stage.setExecutorParams(deserializedExecutorParams);
        logContext(log.atInfo(), stageName).log("Using deserialized executor");
      }
    }

    StageExecutionResult result;
    try {
      logContext(log.atInfo(), stageName).log("Executing stage");
      if (!isUsingDeserializedExecutor) {
        // Start a new stage execution and serialize the executor.
        stageEntity.startExecution(stage);
        stageService.saveStage(stageEntity);
        stageService.saveStageOut(StageOutEntity.startExecution(stageEntity));
      }

      result = stage.getExecutor().execute(pipelineName, getProcessId(), stage);

      if (!isUsingDeserializedExecutor && result.isActive()) {
        // Serialize an active executor. For example, an asynchronous executor may have
        // assigned a job id that is associated with an external execution service.
        stageEntity.serializeExecution(stage);
        stageService.saveStage(stageEntity);
      }
    } catch (Exception ex) {
      result = StageExecutionResult.error();
      result.addExceptionAttribute(ex);
    }

    if (result.isActive()) {
      // If the execution state is active then we have an asynchronous executor.
      while (true) {
        try {
          logContext(log.atInfo(), stageName)
              .log("Waiting for asynchronous stage execution to complete");
          // Execute the stage repeatedly until it is no longer active.
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
          return;
        }
      }
    }

    // Stage has been executed.
    stageEntity.endExecution(result);
    stageService.saveStage(stageEntity);
    stageService.saveStageOut(StageOutEntity.endExecution(stageEntity, result));
    stage.incrementImmediateExecutionCount();
    if (result.isSuccess()) {
      stageSuccessCount.incrementAndGet();
      logContext(log.atInfo(), stageEntity.getStageName())
          .with(LogKey.STAGE_EXECUTION_RESULT_TYPE, stageEntity.getResultType())
          .with(LogKey.STAGE_EXECUTION_COUNT, stageEntity.getExecutionCount())
          .log("Stage executed successfully.");
      invalidateDependentStages(stage);
    } else {
      // Send email after failed stage execution.
      mailService.sendStageExecutionMessage(pipelineName, process, stage);
      stageFailedCount.incrementAndGet();
      logContext(log.atSevere(), stageEntity.getStageName())
          .with(LogKey.STAGE_EXECUTION_RESULT_TYPE, stageEntity.getResultType())
          .with(LogKey.STAGE_EXECUTION_COUNT, stageEntity.getExecutionCount())
          .log("Stage execution failed");
    }
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

  private void invalidateDependentStages(Stage from) {
    for (Stage stage : DependencyResolver.getDependentStages(stages, from)) {
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
