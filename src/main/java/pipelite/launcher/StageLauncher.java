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
import java.time.ZonedDateTime;
import lombok.extern.flogger.Flogger;
import org.springframework.util.Assert;
import pipelite.exception.PipeliteException;
import pipelite.log.LogKey;
import pipelite.process.Process;
import pipelite.service.StageService;
import pipelite.stage.Stage;
import pipelite.stage.executor.StageExecutor;
import pipelite.stage.executor.StageExecutorRequest;
import pipelite.stage.executor.StageExecutorResult;
import pipelite.stage.parameters.ExecutorParameters;
import pipelite.time.Time;

@Flogger
/** Executes a stage and returns the stage execution result. */
public class StageLauncher {

  private final StageService stageService;
  private final String pipelineName;
  private final Process process;
  private final Stage stage;
  private final ZonedDateTime timeout;

  private static final Duration POLL_FREQUENCY = Duration.ofSeconds(10);

  public StageLauncher(
      StageService stageService, String pipelineName, Process process, Stage stage) {
    Assert.notNull(stageService, "Missing stage service");
    Assert.notNull(pipelineName, "Missing pipeline name");
    Assert.notNull(process, "Missing process");
    Assert.notNull(process.getProcessEntity(), "Missing process entity");
    Assert.notNull(stage, "Missing stage");
    Assert.notNull(stage.getStageEntity(), "Missing stage entity");
    this.stageService = stageService;
    this.pipelineName = pipelineName;
    this.process = process;
    this.stage = stage;
    this.timeout = ZonedDateTime.now().plus(getTimeout(stage));
  }

  /**
   * Returns the stage execution timeout.
   *
   * @return The stage execution timeout.
   */
  public static Duration getTimeout(Stage stage) {
    ExecutorParameters executorParams = stage.getExecutor().getExecutorParams();
    if (executorParams.getTimeout() != null) {
      return executorParams.getTimeout();
    }
    return ExecutorParameters.DEFAULT_TIMEOUT;
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
   * @throws PipeliteException if could not execute the stage
   */
  public StageExecutorResult run() {
    logContext(log.atInfo()).log("Executing stage");
    StageExecutorResult result;
    try {
      StageExecutor stageExecutor = stage.getExecutor();
      stageExecutor.prepareExecute(stageService.getExecutorContextCache());
      result =
          stageExecutor.execute(
              StageExecutorRequest.builder()
                  .pipelineName(pipelineName)
                  .processId(process.getProcessId())
                  .stage(stage)
                  .build());
      if (result.isActive()) {
        stageService.startAsyncExecution(stage);
        // If the execution state is active then the executor is asynchronous.
        result = pollExecution();
      }
    } catch (PipeliteException ex) {
      throw ex;
    } catch (Exception ex) {
      throw new PipeliteException(ex);
    }
    if (result.isSuccess()) {
      logContext(log.atInfo()).log("Stage execution succeeded");
    } else if (result.isError()) {
      logContext(log.atSevere()).log("Stage execution failed");
    } else {
      throw new PipeliteException("Unexpected stage execution result");
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
        logContext(log.atFine()).log("Waiting asynchronous stage execution to complete");
        result = stage.execute(pipelineName, process.getProcessId());
        if (!result.isActive()) {
          // The asynchronous stage execution has completed.
          return result;
        }
      } catch (PipeliteException ex) {
        throw ex;
      } catch (Exception ex) {
        throw new PipeliteException("Unexpected exception when executing stage", ex);
      }
      if (ZonedDateTime.now().isAfter(timeout)) {
        logContext(log.atSevere()).log("Maximum run time exceeded. Terminating job.");
        try {
          stage.getExecutor().terminate();
        } catch (PipeliteException ex) {
          throw ex;
        } catch (Exception ex) {
          throw new PipeliteException("Unexpected exception when terminating job.", ex);
        }
        return StageExecutorResult.timeoutError();
      }
      Time.waitUntil(POLL_FREQUENCY, timeout);
    }
  }

  public void terminate() {
    stage.getExecutor().terminate();
  }

  private FluentLogger.Api logContext(FluentLogger.Api log) {
    return log.with(LogKey.PIPELINE_NAME, pipelineName)
        .with(LogKey.PROCESS_ID, process.getProcessId())
        .with(LogKey.STAGE_NAME, stage.getStageName())
        .with(LogKey.STAGE_STATE, stage.getStageEntity().getStageState())
        .with(LogKey.STAGE_EXECUTION_COUNT, stage.getStageEntity().getExecutionCount());
  }
}
