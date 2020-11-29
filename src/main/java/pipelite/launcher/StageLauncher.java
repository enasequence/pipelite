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

import com.google.common.flogger.FluentLogger;
import java.time.Duration;
import lombok.extern.flogger.Flogger;
import org.springframework.util.Assert;
import pipelite.configuration.LauncherConfiguration;
import pipelite.configuration.StageConfiguration;
import pipelite.entity.StageEntity;
import pipelite.entity.StageOutEntity;
import pipelite.executor.ConfigurableStageExecutorParameters;
import pipelite.executor.StageExecutor;
import pipelite.executor.StageExecutorParameters;
import pipelite.launcher.dependency.DependencyResolver;
import pipelite.log.LogKey;
import pipelite.process.Process;
import pipelite.service.MailService;
import pipelite.service.StageService;
import pipelite.stage.Stage;
import pipelite.stage.StageExecutionResult;
import pipelite.stage.StageExecutionResultType;

@Flogger
public class StageLauncher {

  private final LauncherConfiguration launcherConfiguration;
  private final StageConfiguration stageConfiguration;
  private final StageService stageService;
  private final MailService mailService;
  private final String pipelineName;
  private final Process process;
  private final Stage stage;

  private final Duration stagePollFrequency;

  public StageLauncher(
      LauncherConfiguration launcherConfiguration,
      StageConfiguration stageConfiguration,
      StageService stageService,
      MailService mailService,
      String pipelineName,
      Process process,
      Stage stage) {
    this.launcherConfiguration = launcherConfiguration;
    this.stageConfiguration = stageConfiguration;
    this.stageService = stageService;
    this.mailService = mailService;
    this.pipelineName = pipelineName;
    this.process = process;
    this.stage = stage;
    Assert.notNull(launcherConfiguration, "Missing launcher configuration");
    Assert.notNull(stageConfiguration, "Missing stage configuration");
    Assert.notNull(stageService, "Missing stage service");
    Assert.notNull(mailService, "Missing mail service");
    Assert.notNull(pipelineName, "Missing pipeline name");
    Assert.notNull(process, "Missing process");
    Assert.notNull(process.getProcessEntity(), "Missing process entity");
    Assert.notNull(stage, "Missing stage");
    Assert.notNull(stage.getStageEntity(), "Missing stage entity");
    if (launcherConfiguration.getStagePollFrequency() != null) {
      this.stagePollFrequency = launcherConfiguration.getStagePollFrequency();
    } else {
      this.stagePollFrequency = LauncherConfiguration.DEFAULT_STAGE_POLL_FREQUENCY;
    }
  }

  /**
   * Returns the maximum number of retries for the stage.
   *
   * @return The maximum number of retries.
   */
  public static int getMaximumRetries(Stage stage) {
    int maximumRetries = ConfigurableStageExecutorParameters.DEFAULT_MAX_RETRIES;
    if (stage.getExecutorParams().getMaximumRetries() != null) {
      maximumRetries = stage.getExecutorParams().getMaximumRetries();
    }
    return Math.max(0, maximumRetries);
  }

  /**
   * Returns the maximum number of immediate retries for the stage.
   *
   * @return The maximum number of immediate retries.
   */
  public static int getImmediateRetries(Stage stage) {
    int immediateRetries = ConfigurableStageExecutorParameters.DEFAULT_IMMEDIATE_RETRIES;
    if (stage.getExecutorParams().getImmediateRetries() != null) {
      immediateRetries = stage.getExecutorParams().getImmediateRetries();
    }
    return Math.min(Math.max(0, immediateRetries), getMaximumRetries(stage));
  }

  /**
   * Executes the stage and returns the stage execution result.
   *
   * @return The stage execution result.
   */
  public StageExecutionResult run() {
    logContext(log.atInfo()).log("Executing stage");
    StageEntity stageEntity = stage.getStageEntity();
    stage.setStageEntity(stageEntity);
    StageExecutionResult result;
    try {
      startExecution();
      result = stage.getExecutor().execute(pipelineName, process.getProcessId(), stage);
      if (result.isActive()) {
        // If the execution state is active then the executor is asynchronous.
        result = pollExecution();
      }
    } catch (Exception ex) {
      result = StageExecutionResult.error();
      result.addExceptionAttribute(ex);
    }
    endExecution(result);
    return result;
  }

  // TODO: test deserialization of active executor
  private void startExecution() {
    StageEntity stageEntity = stage.getStageEntity();
    // Attempt to deserialize stage executor to allow an asynchronous executor to continue executing
    // an active stage.
    if (StageExecutionResultType.isActive(stageEntity.getResultType())
        && stageEntity.getExecutorName() != null
        && stageEntity.getExecutorData() != null
        && stageEntity.getExecutorParams() != null) {
      StageExecutor deserializedExecutor = deserializeExecutor();
      StageExecutorParameters deserializedExecutorParams = deserializeExecutorParameters();
      if (deserializedExecutor != null && deserializedExecutorParams != null) {
        logContext(log.atInfo()).log("Using deserialized executor");
        stage.setExecutor(deserializedExecutor);
        stage.setExecutorParams(deserializedExecutorParams);
        return;
      }
    }
    stageService.startExecution(stage);
  }

  private StageExecutionResult pollExecution() {
    StageExecutionResult result;
    while (true) {
      try {
        logContext(log.atInfo()).log("Waiting asynchronous stage execution to complete");
        result = stage.getExecutor().execute(pipelineName, process.getProcessId(), stage);
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

  private void endExecution(StageExecutionResult result) {
    stage.incrementImmediateExecutionCount();
    StageEntity stageEntity = stage.getStageEntity();
    stageService.endExecution(stage, result);
    if (result.isSuccess()) {
      logContext(log.atInfo())
          .with(LogKey.STAGE_EXECUTION_RESULT_TYPE, stageEntity.getResultType())
          .with(LogKey.STAGE_EXECUTION_COUNT, stageEntity.getExecutionCount())
          .log("Stage executed successfully.");
      invalidateDependentStages(stage);
    } else if (result.isError()) {
      logContext(log.atSevere())
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

  private StageExecutor deserializeExecutor() {
    StageEntity stageEntity = stage.getStageEntity();
    String stageName = stage.getStageName();
    try {
      return StageExecutor.deserialize(
          stageEntity.getExecutorName(), stageEntity.getExecutorData());
    } catch (Exception ex) {
      logContext(log.atSevere())
          .withCause(ex)
          .log("Failed to deserialize executor: %s", stageEntity.getExecutorName());
    }
    return null;
  }

  private StageExecutorParameters deserializeExecutorParameters() {
    StageEntity stageEntity = stage.getStageEntity();
    String stageName = stage.getStageName();
    try {
      return StageExecutorParameters.deserialize(stageEntity.getExecutorParams());
    } catch (Exception ex) {
      logContext(log.atSevere())
          .withCause(ex)
          .log("Failed to deserialize executor parameters: %s", stageEntity.getExecutorName());
    }
    return null;
  }

  private void invalidateDependentStages(Stage from) {
    for (Stage stage : DependencyResolver.getDependentStages(process.getStages(), from)) {
      StageEntity stageEntity = stage.getStageEntity();
      stageEntity.resetExecution();
      stageService.saveStage(stageEntity);
      stageService.saveStageOut(StageOutEntity.resetExecution(stageEntity));
    }
  }

  private FluentLogger.Api logContext(FluentLogger.Api log) {
    return log.with(LogKey.PIPELINE_NAME, pipelineName)
        .with(LogKey.PROCESS_ID, process.getProcessId())
        .with(LogKey.STAGE_NAME, stage.getStageName());
  }
}
