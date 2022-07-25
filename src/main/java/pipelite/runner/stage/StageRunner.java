/*
 * Copyright 2020-2022 EMBL - European Bioinformatics Institute
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package pipelite.runner.stage;

import com.google.common.flogger.FluentLogger;
import java.time.Duration;
import java.time.ZonedDateTime;
import lombok.extern.flogger.Flogger;
import org.springframework.util.Assert;
import pipelite.entity.field.StageState;
import pipelite.error.InternalErrorHandler;
import pipelite.executor.AsyncExecutor;
import pipelite.executor.TimeoutExecutor;
import pipelite.log.LogKey;
import pipelite.metrics.PipeliteMetrics;
import pipelite.process.Process;
import pipelite.service.PipeliteServices;
import pipelite.stage.Stage;
import pipelite.stage.executor.StageExecutor;
import pipelite.stage.executor.StageExecutorResult;
import pipelite.stage.parameters.ExecutorParameters;

@Flogger
/** Executes a stage and returns the stage execution result. */
public class StageRunner {

  private final PipeliteServices pipeliteServices;
  private final PipeliteMetrics pipeliteMetrics;
  private final String pipelineName;
  private final Process process;
  private final Stage stage;
  private final ZonedDateTime timeout;
  private final InternalErrorHandler internalErrorHandler;

  private enum StageRunnerState {
    PREPARE,
    EXECUTE,
    COMPLETED
  }

  private StageRunnerState stageRunnerState = StageRunnerState.PREPARE;

  /** Completed stage execution result. */
  private StageExecutorResult stageExecutorResult;

  public StageRunner(
      PipeliteServices pipeliteServices,
      PipeliteMetrics pipeliteMetrics,
      String serviceName,
      String pipelineName,
      Process process,
      Stage stage) {
    Assert.notNull(pipeliteServices, "Missing pipelite services");
    Assert.notNull(pipeliteMetrics, "Missing pipelite metrics");
    Assert.notNull(serviceName, "Missing service name");
    Assert.notNull(pipelineName, "Missing pipeline name");
    Assert.notNull(process, "Missing process");
    Assert.notNull(process.getProcessEntity(), "Missing process entity");
    Assert.notNull(stage, "Missing stage");
    Assert.notNull(stage.getStageEntity(), "Missing stage entity");
    this.pipeliteServices = pipeliteServices;
    this.pipeliteMetrics = pipeliteMetrics;
    this.pipelineName = pipelineName;
    this.process = process;
    this.stage = stage;
    this.timeout = ZonedDateTime.now().plus(getTimeout(stage));
    this.internalErrorHandler =
        new InternalErrorHandler(
            pipeliteServices.internalError(),
            pipelineName,
            process.getProcessId(),
            stage.getStageName(),
            this);
    if (stage.getStageEntity().getStageState() == StageState.ACTIVE) {
      // Continue active execution.
    } else {
      // Reset executor state.
      if (stage.getStageEntity().getStageState() != StageState.PENDING
          && stage.getExecutor() instanceof AsyncExecutor) {
        logContext(log.atInfo()).log("Reset async stage executor");
        StageExecutor.resetAsyncExecutorState(stage);
      }
      // Start execution.
      pipeliteServices.stage().startExecution(stage);
    }
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
   * Called until the stage execution has been completed. An internal error result will be returned
   * if an unexpected exception is thrown.
   *
   * @return the stage execution result.
   */
  public StageExecutorResult runOneIteration() {
    if (stageRunnerState == StageRunnerState.COMPLETED) {
      return stageExecutorResult;
    }

    // Unexpected exceptions are logged as internal errors and the stage execution is considered
    // failed.
    internalErrorHandler.execute(
        () -> runStage(),
        (ex) -> {
          // The stage execution failed because an unexpected exception was thrown.
          logContext(log.atSevere())
              .withCause(ex)
              .log("Stage failed because of an unexpected exception");
          endStageExecution(StageExecutorResult.internalError().stageLog(ex));
        });

    return (stageExecutorResult != null) ? stageExecutorResult : StageExecutorResult.active();
  }

  public void runStage() {
    ZonedDateTime runOneIterationStartTime = ZonedDateTime.now();

    if (stageRunnerState == StageRunnerState.PREPARE) {
      prepareStageExecution();
      stageRunnerState = StageRunnerState.EXECUTE;
    }

    if (stageRunnerState == StageRunnerState.EXECUTE) {
      StageExecutorResult result = executeStage();
      if (result.isCompleted()) {
        endStageExecution(result);
      }
    }

    pipeliteMetrics
        .process(pipelineName)
        .stage(stage.getStageName())
        .runner()
        .endRunOneIteration(runOneIterationStartTime);
  }

  private void prepareStageExecution() {
    logContext(log.atInfo()).log("Preparing stage execution");
    stage
        .getExecutor()
        .prepareExecution(pipeliteServices, pipelineName, process.getProcessId(), stage);
  }

  private StageExecutorResult executeStage() {
    StageExecutorResult result = stage.execute();

    if (result.isSubmitted()) {
      // Save submitted stage.
      logContext(log.atInfo()).log("Saving submitted stage");
      pipeliteServices.stage().saveStage(stage);
    } else if (result.isActive()) {
      // Terminate stage if maximum run time was exceeded.
      boolean isTimeoutExecutor = stage.getExecutor() instanceof TimeoutExecutor;
      if (!isTimeoutExecutor && ZonedDateTime.now().isAfter(timeout)) {
        logContext(log.atSevere()).log("Terminating stage because maximum run time was exceeded");

        // Unexpected exceptions are logged as internal errors but otherwise ignored when
        // terminating stage.
        internalErrorHandler.execute(() -> stage.getExecutor().terminate());
        result = StageExecutorResult.timeoutError();
      }
    }
    return result;
  }

  private void endStageExecution(StageExecutorResult result) {
    Assert.notNull(result, "Missing stage executor result");
    logContext(log.atInfo()).log("Stage execution finished");

    stageExecutorResult = result;
    stageRunnerState = StageRunnerState.COMPLETED;

    pipeliteServices.stage().endExecution(stage, stageExecutorResult);

    if (!stageExecutorResult.isSuccess()) {
      pipeliteServices.mail().sendStageExecutionMessage(process, stage);
    }

    pipeliteMetrics
        .process(pipelineName)
        .stage(stage.getStageName())
        .runner()
        .endStageExecution(stageExecutorResult);
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
