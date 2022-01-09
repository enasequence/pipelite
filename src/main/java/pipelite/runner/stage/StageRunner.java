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
import pipelite.log.LogKey;
import pipelite.metrics.PipeliteMetrics;
import pipelite.process.Process;
import pipelite.service.PipeliteServices;
import pipelite.stage.Stage;
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
  private StageExecutorResult executorResult;

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
      executeStage(resultCallback);

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
    stage.getExecutor().prepareExecute(pipeliteServices.stage().getExecutorDescribeJobsCache());
  }

  private void executeStage(StageRunnerResultCallback resultCallback) {
    try {
      executorResult = stage.execute(pipelineName, process.getProcessId());

      if (executorResult.isActive() && ZonedDateTime.now().isAfter(timeout)) {
        logContext(log.atSevere())
            .log("Maximum stage execution time exceeded. Terminating stage execution.");
        stage.getExecutor().terminate();
        executorResult = StageExecutorResult.timeoutError();
      }
    } catch (Exception ex) {
      executorResult = StageExecutorResult.internalError(ex);
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

    if (executorResult.isSubmitted()) {
      logContext(log.atFine()).log("Started asynchronous stage execution");
      pipeliteServices.stage().startAsyncExecution(stage);
    } else if (executorResult.isActive()) {
      logContext(log.atFine()).log("Waiting asynchronous stage execution to complete");
    } else if (executorResult.isSuccess() || executorResult.isError()) {
      logContext(log.atFine())
          .log("Stage execution " + (executorResult.isSuccess() ? "succeeded" : "failed"));
      endStageExecution(stage, executorResult);
      resultCallback.accept(executorResult);
    }
  }

  private void endStageExecution(Stage stage, StageExecutorResult result) {
    pipeliteServices.stage().endExecution(stage, result);
    if (!result.isSuccess()) {
      pipeliteServices.mail().sendStageExecutionMessage(process, stage);
    }
    pipeliteMetrics.pipeline(pipelineName).stage().endStageExecution(result);
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

  public StageExecutorResult getExecutorResult() {
    return executorResult;
  }

  private FluentLogger.Api logContext(FluentLogger.Api log) {
    return log.with(LogKey.PIPELINE_NAME, pipelineName)
        .with(LogKey.PROCESS_ID, process.getProcessId())
        .with(LogKey.STAGE_NAME, stage.getStageName())
        .with(LogKey.STAGE_STATE, stage.getStageEntity().getStageState())
        .with(LogKey.STAGE_EXECUTION_COUNT, stage.getStageEntity().getExecutionCount());
  }
}
