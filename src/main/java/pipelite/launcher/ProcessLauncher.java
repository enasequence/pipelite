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

import static pipelite.stage.ConfigurableStageParameters.DEFAULT_IMMEDIATE_RETRIES;
import static pipelite.stage.ConfigurableStageParameters.DEFAULT_MAX_RETRIES;
import static pipelite.stage.StageExecutionResultType.*;

import com.google.common.flogger.FluentLogger;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import lombok.extern.flogger.Flogger;
import pipelite.configuration.*;
import pipelite.entity.ProcessEntity;
import pipelite.entity.StageEntity;
import pipelite.executor.StageExecutor;
import pipelite.launcher.dependency.DependencyResolver;
import pipelite.log.LogKey;
import pipelite.process.Process;
import pipelite.process.ProcessState;
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
  private final String pipelineName;
  private final Process process;
  private final ProcessEntity processEntity;

  private final List<StageExecution> stageExecutions;
  private final ExecutorService executorService;
  private final Set<String> activeStages = ConcurrentHashMap.newKeySet();
  private final Duration stageLaunchFrequency;
  private final Duration stagePollFrequency;

  private final AtomicLong stageFailedCount = new AtomicLong(0);
  private final AtomicLong stageSuccessCount = new AtomicLong(0);

  public ProcessLauncher(
      LauncherConfiguration launcherConfiguration,
      StageConfiguration stageConfiguration,
      ProcessService processService,
      StageService stageService,
      String pipelineName,
      Process process,
      ProcessEntity processEntity) {

    this.launcherConfiguration = launcherConfiguration;
    this.stageConfiguration = stageConfiguration;
    this.processService = processService;
    this.stageService = stageService;
    this.pipelineName = pipelineName;
    this.process = process;
    this.processEntity = processEntity;

    this.stageExecutions = new ArrayList<>();
    this.executorService = Executors.newCachedThreadPool();

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
  }

  public static class StageExecution {
    private final Stage stage;
    private final StageEntity stageEntity;
    private AtomicInteger immediateExecutionCount = new AtomicInteger(0);

    public StageExecution(Stage stage, StageEntity stageEntity) {
      this.stage = stage;
      this.stageEntity = stageEntity;
    }

    public Stage getStage() {
      return stage;
    }

    public StageEntity getStageEntity() {
      return stageEntity;
    }

    public int getImmediateExecutionCount() {
      return immediateExecutionCount.get();
    }

    public void incrementImmediateExecutionCount() {
      immediateExecutionCount.incrementAndGet();
    }
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
      stage.getStageParameters().add(stageConfiguration);

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

      // Creates an executable stage consisting of the stage and stage entity.
      stageExecutions.add(new StageExecution(stage, stageEntity.get()));
    }
  }

  public static ProcessState evaluateProcessState(List<StageExecution> stageExecutions) {
    int successCount = 0;
    int activeCount = 0;
    int errorCount = 0;
    for (StageExecution stageExecution : stageExecutions) {
      Stage stage = stageExecution.stage;
      StageEntity stageEntity = stageExecution.stageEntity;
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
      if (!DependencyResolver.getExecutableStages(stageExecutions).isEmpty()) {
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
    if (stage.getStageParameters().getMaximumRetries() != null) {
      maximumRetries = stage.getStageParameters().getMaximumRetries();
    }
    return Math.max(0, maximumRetries);
  }

  public static int getImmediateRetries(Stage stage) {
    int immediateRetries = DEFAULT_IMMEDIATE_RETRIES;
    if (stage.getStageParameters().getImmediateRetries() != null) {
      immediateRetries = stage.getStageParameters().getImmediateRetries();
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

      List<StageExecution> executableStages =
          DependencyResolver.getExecutableStages(stageExecutions);
      if (activeStages.isEmpty() && executableStages.isEmpty()) {
        logContext(log.atInfo()).log("No active or executable stages");
        return;
      }

      for (StageExecution stageExecution : executableStages) {
        Stage stage = stageExecution.stage;
        String stageName = stage.getStageName();

        if (activeStages.contains(stageName)) {
          continue;
        }

        if (stage.getDependsOn() != null) {
          String dependsOnStageName = stage.getDependsOn().getStageName();
          if (dependsOnStageName != null && activeStages.contains(dependsOnStageName)) {
            continue;
          }
        }

        activeStages.add(stageName);
        executorService.execute(
            () -> {
              try {
                executeStage(stageExecution);
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

    processEntity.updateExecution(evaluateProcessState(stageExecutions));
    processService.saveProcess(processEntity);
    return processEntity.getState();
  }

  private void executeStage(StageExecution stageExecution) {
    Stage stage = stageExecution.stage;
    StageEntity stageEntity = stageExecution.stageEntity;
    String stageName = stage.getStageName();

    logContext(log.atInfo(), stageName).log("Preparing to execute stage");

    // If the stage executor has been serialized we should use it to allow an active stage
    // execution to continue. For example, an asynchronous executor may contain a job id that
    // is associated with an external execution service.

    StageExecutor deserializedExecutor = deserializeActiveExecutor(stageExecution);
    boolean isDeserializedExecutor = deserializedExecutor != null;

    if (isDeserializedExecutor) {
      // Use deserialized executor.
      stage.setExecutor(deserializedExecutor);
      logContext(log.atInfo(), stageName).log("Using deserialized executor");
    }

    StageExecutionResult result;

    try {
      logContext(log.atInfo(), stageName).log("Executing stage");

      if (!isDeserializedExecutor) {
        // Start a new stage execution and serialize the executor.
        stageEntity.startExecution(stage);
        stageService.saveStage(stageEntity);
      }

      result = stage.getExecutor().execute(pipelineName, getProcessId(), stage);

      if (!isDeserializedExecutor && result.isActive()) {
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
    stageExecution.incrementImmediateExecutionCount();

    if (result.isSuccess()) {
      stageSuccessCount.incrementAndGet();
      logContext(log.atInfo(), stageEntity.getStageName())
          .with(LogKey.STAGE_EXECUTION_RESULT_TYPE, stageEntity.getResultType())
          .with(LogKey.STAGE_EXECUTION_COUNT, stageEntity.getExecutionCount())
          .log("Stage executed successfully.");
      invalidateDependentStages(stageExecution);
    } else {
      stageFailedCount.incrementAndGet();
      logContext(log.atSevere(), stageEntity.getStageName())
          .with(LogKey.STAGE_EXECUTION_RESULT_TYPE, stageEntity.getResultType())
          .with(LogKey.STAGE_EXECUTION_COUNT, stageEntity.getExecutionCount())
          .log("Stage execution failed");
    }
  }

  private StageExecutor deserializeActiveExecutor(StageExecution stageExecution) {
    Stage stage = stageExecution.stage;
    StageEntity stageEntity = stageExecution.stageEntity;
    String stageName = stage.getStageName();

    if (stageEntity.getResultType() == ACTIVE
        && stageEntity.getExecutorName() != null
        && stageEntity.getExecutorData() != null) {
      try {
        return StageExecutor.deserialize(
            stageEntity.getExecutorName(), stageEntity.getExecutorData());
      } catch (Exception ex) {
        logContext(log.atSevere(), stageName)
            .withCause(ex)
            .log("Failed to deserialize executor: %s", stageEntity.getExecutorName());
      }
    }
    return null;
  }

  private void invalidateDependentStages(StageExecution from) {
    for (StageExecution stageExecution :
        DependencyResolver.getDependentStages(stageExecutions, from)) {
      StageEntity stageEntity = stageExecution.stageEntity;
      stageEntity.resetExecution();
      stageService.saveStage(stageEntity);
    }
  }

  public String getProcessId() {
    return process.getProcessId();
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
