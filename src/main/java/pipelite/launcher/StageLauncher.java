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
import pipelite.executor.ConfigurableStageExecutorParameters;
import pipelite.log.LogKey;
import pipelite.process.Process;
import pipelite.stage.Stage;
import pipelite.stage.StageExecutionResult;

@Flogger
/** Executes a stage and returns the stage execution result. */
public class StageLauncher {

  private final LauncherConfiguration launcherConfiguration;
  private final StageConfiguration stageConfiguration;
  private final String pipelineName;
  private final Process process;
  private final Stage stage;
  private final Duration stagePollFrequency;

  public StageLauncher(
      LauncherConfiguration launcherConfiguration,
      StageConfiguration stageConfiguration,
      String pipelineName,
      Process process,
      Stage stage) {
    this.launcherConfiguration = launcherConfiguration;
    this.stageConfiguration = stageConfiguration;
    this.pipelineName = pipelineName;
    this.process = process;
    this.stage = stage;
    Assert.notNull(launcherConfiguration, "Missing launcher configuration");
    Assert.notNull(stageConfiguration, "Missing stage configuration");
    Assert.notNull(pipelineName, "Missing pipeline name");
    Assert.notNull(process, "Missing process");
    Assert.notNull(process.getProcessEntity(), "Missing process entity");
    Assert.notNull(stage, "Missing stage");
    Assert.notNull(stage.getStageEntity(), "Missing stage entity");
    this.stagePollFrequency = launcherConfiguration.getStagePollFrequency();
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
    StageExecutionResult result;
    try {
      result = stage.getExecutor().execute(pipelineName, process.getProcessId(), stage);
      if (result.isActive()) {
        // If the execution state is active then the executor is asynchronous.
        result = pollExecution();
      }
    } catch (Exception ex) {
      result = StageExecutionResult.error(ex);
    }
    if (result.isSuccess()) {
      logContext(log.atInfo()).log("Stage execution succeeded");
    } else if (result.isError()) {
      logContext(log.atSevere()).log("Stage execution failed");
    } else {
      logContext(log.atSevere()).log("Unexpected stage execution result type");
      throw new RuntimeException("Unexpected stage execution result type");
    }
    return result;
  }

  /**
   * Wait for an asynchronous executor to complete.
   *
   * @return the stage execution result
   */
  StageExecutionResult pollExecution() {
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
        result = StageExecutionResult.error(ex);
        break;
      }
      try {
        Thread.sleep(stagePollFrequency.toMillis());
      } catch (InterruptedException ex) {
        log.atSevere().log("Stage launcher was interrupted");
        Thread.currentThread().interrupt();
        throw new RuntimeException(ex);
      }
    }
    return result;
  }

  private FluentLogger.Api logContext(FluentLogger.Api log) {
    return log.with(LogKey.PIPELINE_NAME, pipelineName)
        .with(LogKey.PROCESS_ID, process.getProcessId())
        .with(LogKey.STAGE_NAME, stage.getStageName())
        .with(LogKey.STAGE_EXECUTION_RESULT_TYPE, stage.getStageEntity().getResultType())
        .with(LogKey.STAGE_EXECUTION_COUNT, stage.getStageEntity().getExecutionCount());
  }
}
