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
package pipelite.runner.stage;

import com.google.common.flogger.FluentLogger;
import java.time.Duration;
import java.time.ZonedDateTime;
import lombok.extern.flogger.Flogger;
import org.springframework.util.Assert;
import pipelite.error.InternalErrorHandler;
import pipelite.exception.PipeliteException;
import pipelite.executor.AsyncExecutor;
import pipelite.log.LogKey;
import pipelite.metrics.PipeliteMetrics;
import pipelite.process.Process;
import pipelite.service.PipeliteServices;
import pipelite.stage.Stage;
import pipelite.stage.StageState;
import pipelite.stage.executor.StageExecutor;
import pipelite.stage.executor.StageExecutorResult;
import pipelite.stage.executor.StageExecutorResultCallback;
import pipelite.stage.parameters.ExecutorParameters;

@Flogger
/** Executes a stage and returns the stage execution result. */
public class StageRunner {

  private final PipeliteServices pipeliteServices;
  private final PipeliteMetrics pipeliteMetrics;
  private final String pipelineName;
  private final Process process;
  private final Stage stage;
  private ZonedDateTime startTime;
  private final ZonedDateTime timeout;
  private final InternalErrorHandler internalErrorHandler;

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
    if (stage.getStageEntity().getStageState() != StageState.ACTIVE) {
      // Reset executor state.
      if (stage.getExecutor() instanceof AsyncExecutor) {
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
   * Called until the stage has been executed and the result callback has been called with success
   * or error execution result.
   *
   * @param processRunnerResultCallback the result callback from process runner
   */
  public void runOneIteration(StageExecutorResultCallback processRunnerResultCallback) {
    internalErrorHandler.execute(
        () -> {
          boolean isFirstIteration = startTime == null;
          ZonedDateTime runOneIterationStartTime = ZonedDateTime.now();
          if (isFirstIteration) {
            startTime = ZonedDateTime.now();
            prepareStageExecution();
          }
          executeStage(processRunnerResultCallback);
          pipeliteMetrics
              .process(pipelineName)
              .stage(stage.getStageName())
              .runner()
              .endRunOneIteration(runOneIterationStartTime);
        });
  }

  private void prepareStageExecution() {
    logContext(log.atInfo()).log("Executing stage");
    stage
        .getExecutor()
        .prepareExecution(
            pipeliteServices, pipelineName, process.getProcessId(), stage.getStageName());
  }

  private void executeStage(StageExecutorResultCallback processRunnerResultCallback) {
    StageExecutorResultCallback stageRunnerResultCallback =
        (executorResult) ->
            internalErrorHandler.execute(
                () -> {
                  if (executorResult == null) {
                    throw new PipeliteException("Missing executor result");
                  }
                  if (executorResult.isSubmitted()) {
                    logContext(log.atFine()).log("Submitted asynchronous stage execution");
                    pipeliteServices.stage().saveStage(stage);
                  } else if (executorResult.isActive()) {
                    if (ZonedDateTime.now().isAfter(timeout)) {
                      logContext(log.atSevere()).log("Maximum stage execution time exceeded.");
                      // Terminate executor.
                      internalErrorHandler.execute(() -> stage.getExecutor().terminate());
                      endStageExecution(
                          processRunnerResultCallback, StageExecutorResult.timeoutError());
                    } else {
                      logContext(log.atFiner())
                          .log("Waiting asynchronous stage execution to complete");
                    }
                  } else if (executorResult.isSuccess() || executorResult.isError()) {
                    endStageExecution(processRunnerResultCallback, executorResult);
                  }
                },
                (ex) ->
                    endStageExecution(
                        processRunnerResultCallback, StageExecutorResult.internalError(ex)));
    executeStage(processRunnerResultCallback, stageRunnerResultCallback);
  }

  private void executeStage(
      StageExecutorResultCallback processRunnerResultCallback,
      StageExecutorResultCallback stageRunnerResultCallback) {
    internalErrorHandler.execute(
        () -> stage.execute(pipelineName, process.getProcessId(), stageRunnerResultCallback),
        (ex) ->
            endStageExecution(processRunnerResultCallback, StageExecutorResult.internalError(ex)));
  }

  private void endStageExecution(
      StageExecutorResultCallback processRunnerResultCallback, StageExecutorResult executorResult) {
    internalErrorHandler.execute(
        () -> {
          logContext(log.atFine())
              .log("Stage execution ended with " + executorResult.getExecutorState().name());
          pipeliteServices.stage().endExecution(stage, executorResult);
          internalErrorHandler.execute(() -> processRunnerResultCallback.accept(executorResult));
          if (!executorResult.isSuccess()) {
            pipeliteServices.mail().sendStageExecutionMessage(process, stage);
          }
          pipeliteMetrics
              .process(pipelineName)
              .stage(stage.getStageName())
              .runner()
              .endStageExecution(executorResult);
        });
  }

  public void terminate() {
    stage.getExecutor().terminate();
  }

  public Process getProcess() {
    return process;
  }

  public Stage getStage() {
    return stage;
  }

  private FluentLogger.Api logContext(FluentLogger.Api log) {
    return log.with(LogKey.PIPELINE_NAME, pipelineName)
        .with(LogKey.PROCESS_ID, process.getProcessId())
        .with(LogKey.STAGE_NAME, stage.getStageName())
        .with(LogKey.STAGE_STATE, stage.getStageEntity().getStageState())
        .with(LogKey.STAGE_EXECUTION_COUNT, stage.getStageEntity().getExecutionCount());
  }
}
