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
import pipelite.exception.PipeliteException;
import pipelite.log.LogKey;
import pipelite.metrics.PipeliteMetrics;
import pipelite.process.Process;
import pipelite.service.PipeliteServices;
import pipelite.stage.Stage;
import pipelite.stage.StageState;
import pipelite.stage.executor.StageExecutorResult;
import pipelite.stage.parameters.ExecutorParameters;

@Flogger
/** Executes a stage and returns the stage execution result. */
public class StageRunner {

  private final PipeliteServices pipeliteServices;
  private final PipeliteMetrics pipeliteMetrics;
  private final String serviceName;
  private final String pipelineName;
  private final Process process;
  private final Stage stage;
  private ZonedDateTime startTime;
  private final ZonedDateTime timeout;

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
    this.serviceName = serviceName;
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
   * Called until the stage has been executed and the result callback has been called.
   *
   * @param resultCallback stage runner result callback
   */
  public void runOneIteration(StageRunnerResultCallback resultCallback) {
    boolean isFirstIteration = startTime == null;
    ZonedDateTime runOneIterationStartTime = ZonedDateTime.now();

    try {
      if (isFirstIteration) {
        startTime = ZonedDateTime.now();
        startStageExecution();
      }
      executeStage(isFirstIteration, resultCallback);

      pipeliteMetrics
          .getStageRunnerOneIterationTimer()
          .record(Duration.between(runOneIterationStartTime, ZonedDateTime.now()));
    } catch (Exception ex) {
      pipeliteServices
          .internalError()
          .saveInternalError(
              serviceName, pipelineName, process.getProcessId(), this.getClass(), ex);
    }
  }

  private void startStageExecution() {
    logContext(log.atInfo()).log("Executing stage");
    stage.getExecutor().prepareExecute(pipeliteServices.stage().getExecutorContextCache());
  }

  private void executeStage(boolean isFirstIteration, StageRunnerResultCallback resultCallback) {
    StageExecutorResult result;
    try {
      result = stage.execute(pipelineName, process.getProcessId());

      if (result.isPending()) {
        throw new PipeliteException("Invalid stage state: " + StageState.PENDING.name());
      }

      if (result.isActive() && ZonedDateTime.now().isAfter(timeout)) {
        logContext(log.atSevere())
            .log("Maximum stage execution time exceeded. Terminating stage execution.");
        stage.getExecutor().terminate();
        result = StageExecutorResult.timeoutError();
      }
    } catch (Exception ex) {
      result = StageExecutorResult.internalError(ex);
      pipeliteServices
          .internalError()
          .saveInternalError(
              serviceName,
              pipelineName,
              process.getProcessId(),
              stage.getStageName(),
              this.getClass(),
              ex);
    }

    if (result.isActive()) {
      logContext(log.atFine()).log("Waiting asynchronous stage execution to complete");
    } else {
      String executorType = isFirstIteration ? "Synchronous" : "Asynchronous";
      if (result.isSuccess()) {
        logContext(log.atInfo()).log(executorType + " stage execution succeeded");
      } else if (result.isError()) {
        logContext(log.atInfo()).log(executorType + " stage execution failed");
      }
      resultCallback.accept(result);
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