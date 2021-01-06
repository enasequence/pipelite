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
import pipelite.configuration.ExecutorConfiguration;
import pipelite.log.LogKey;
import pipelite.process.Process;
import pipelite.stage.Stage;
import pipelite.stage.executor.StageExecutorRequest;
import pipelite.stage.executor.StageExecutorResult;
import pipelite.stage.parameters.ExecutorParameters;
import pipelite.time.Time;

@Flogger
/** Executes a stage and returns the stage execution result. */
public class StageLauncher {

  private final ExecutorConfiguration executorConfiguration;
  private final String pipelineName;
  private final Process process;
  private final Stage stage;

  private static final Duration POLL_FREQUENCY = Duration.ofMinutes(1);

  public StageLauncher(
      ExecutorConfiguration executorConfiguration,
      String pipelineName,
      Process process,
      Stage stage) {
    this.executorConfiguration = executorConfiguration;
    this.pipelineName = pipelineName;
    this.process = process;
    this.stage = stage;
    Assert.notNull(executorConfiguration, "Missing stage configuration");
    Assert.notNull(pipelineName, "Missing pipeline name");
    Assert.notNull(process, "Missing process");
    Assert.notNull(process.getProcessEntity(), "Missing process entity");
    Assert.notNull(stage, "Missing stage");
    Assert.notNull(stage.getStageEntity(), "Missing stage entity");
  }

  /**
   * Returns the maximum number of retries for the stage.
   *
   * @return The maximum number of retries.
   */
  public static int getMaximumRetries(Stage stage) {
    ExecutorParameters executorParams = stage.getExecutor().getExecutorParams();
    int maximumRetries = ExecutorParameters.DEFAULT_MAX_RETRIES;
    if (executorParams.getMaximumRetries() != null) {
      maximumRetries = executorParams.getMaximumRetries();
    }
    return Math.max(0, maximumRetries);
  }

  /**
   * Returns the maximum number of immediate retries for the stage.
   *
   * @return The maximum number of immediate retries.
   */
  public static int getImmediateRetries(Stage stage) {
    ExecutorParameters executorParams = stage.getExecutor().getExecutorParams();
    int immediateRetries = ExecutorParameters.DEFAULT_IMMEDIATE_RETRIES;
    if (executorParams.getImmediateRetries() != null) {
      immediateRetries = executorParams.getImmediateRetries();
    }
    return Math.min(Math.max(0, immediateRetries), getMaximumRetries(stage));
  }

  /**
   * Executes the stage and returns the stage execution result.
   *
   * @return The stage execution result.
   */
  public StageExecutorResult run() {
    logContext(log.atInfo()).log("Executing stage");
    StageExecutorResult result;
    try {
      result =
          stage
              .getExecutor()
              .execute(
                  StageExecutorRequest.builder()
                      .pipelineName(pipelineName)
                      .processId(process.getProcessId())
                      .stage(stage)
                      .build());
      if (result.isActive()) {
        // If the execution state is active then the executor is asynchronous.
        result = pollExecution();
      }
    } catch (Exception ex) {
      result = StageExecutorResult.error(ex);
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
  StageExecutorResult pollExecution() {
    StageExecutorResult result;
    while (true) {
      try {
        logContext(log.atInfo()).log("Waiting asynchronous stage execution to complete");
        result =
            stage
                .getExecutor()
                .execute(
                    StageExecutorRequest.builder()
                        .pipelineName(pipelineName)
                        .processId(process.getProcessId())
                        .stage(stage)
                        .build());
        if (!result.isActive()) {
          // The asynchronous stage execution has completed.
          break;
        }
      } catch (Exception ex) {
        result = StageExecutorResult.error(ex);
        break;
      }
      Time.wait(POLL_FREQUENCY);
    }
    return result;
  }

  private FluentLogger.Api logContext(FluentLogger.Api log) {
    return log.with(LogKey.PIPELINE_NAME, pipelineName)
        .with(LogKey.PROCESS_ID, process.getProcessId())
        .with(LogKey.STAGE_NAME, stage.getStageName())
        .with(LogKey.STAGE_EXECUTOR_RESULT_TYPE, stage.getStageEntity().getResultType())
        .with(LogKey.STAGE_EXECUTION_COUNT, stage.getStageEntity().getExecutionCount());
  }
}
