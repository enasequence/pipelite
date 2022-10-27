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
package pipelite.executor;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.flogger.FluentLogger;
import java.time.ZonedDateTime;
import java.util.concurrent.atomic.AtomicReference;
import lombok.Getter;
import lombok.Setter;
import lombok.Value;
import lombok.extern.flogger.Flogger;
import pipelite.error.InternalErrorHandler;
import pipelite.exception.PipeliteException;
import pipelite.executor.describe.DescribeJobs;
import pipelite.executor.describe.context.executor.DefaultExecutorContext;
import pipelite.executor.describe.context.request.DefaultRequestContext;
import pipelite.log.LogKey;
import pipelite.metrics.StageMetrics;
import pipelite.retryable.RetryableExternalAction;
import pipelite.service.PipeliteServices;
import pipelite.stage.Stage;
import pipelite.stage.executor.StageExecutorResult;
import pipelite.stage.parameters.ExecutorParameters;

/** Executes a stage asynchronously. Must be serializable to json. */
@Getter
@Setter
@Flogger
public abstract class AsyncExecutor<
        T extends ExecutorParameters,
        RequestContext extends DefaultRequestContext,
        ExecutorContext extends DefaultExecutorContext<RequestContext>>
    extends AbstractExecutor<T> {

  /**
   * Asynchronous executor job id. Serialize in database to continue execution after service
   * restart.
   */
  private String jobId;

  @JsonIgnore private final String executorName;

  @JsonIgnore private DescribeJobs<RequestContext, ExecutorContext> describeJobs;
  @JsonIgnore private AtomicReference<RequestContext> requestContext = new AtomicReference<>();

  @JsonIgnore private String pipelineName;
  @JsonIgnore private String processId;
  @JsonIgnore private String stageName;

  /** Completed stage execution result. */
  @JsonIgnore private StageExecutorResult stageExecutorResult;

  @JsonIgnore private InternalErrorHandler internalErrorHandler;
  @JsonIgnore private StageMetrics stageMetrics;

  @JsonIgnore private ZonedDateTime execStartTime;
  @JsonIgnore private ZonedDateTime execEndTime;

  public AsyncExecutor(String executorName) {
    this.executorName = executorName;
  }

  @Override
  public void prepareExecution(
      PipeliteServices pipeliteServices, String pipelineName, String processId, Stage stage) {
    super.prepareExecution(pipeliteServices, pipelineName, processId, stage);
    this.describeJobs = prepareDescribeJobs(pipeliteServices);
    this.pipelineName = pipelineName;
    this.processId = processId;
    this.stageName = stage.getStageName();
    this.stageMetrics = pipeliteServices.metrics().process(pipelineName).stage(stageName);
    this.internalErrorHandler =
        new InternalErrorHandler(
            pipeliteServices.internalError(), pipelineName, processId, stageName, this);
  }

  /** Allow retrieval of async job result by setting the DescribeJobs object. */
  protected abstract DescribeJobs<RequestContext, ExecutorContext> prepareDescribeJobs(
      PipeliteServices pipeliteServices);

  /** Allow retrieval async job result by setting the RequestContext object used by DescribeJobs. */
  protected abstract RequestContext prepareRequestContext();

  /**
   * Returns the request context used by DescribeJobs to retrieve the async job result. The job must
   * be submitted and the job id must be set before the request context is created.
   *
   * @return the request context used by DescribeJobs.
   */
  protected final RequestContext getRequestContext() {
    if (requestContext.get() == null) {
      if (jobId == null) {
        throw new PipeliteException("Failed to create request context because job is missing");
      }
      requestContext.compareAndSet(null, prepareRequestContext());
    }
    return requestContext.get();
  }

  @Override
  @JsonIgnore
  public boolean isSubmitted() {
    return jobId != null;
  }

  /** Prepares the async job for submission. */
  protected void prepareJob() {}

  @Value
  protected static class SubmitJobResult {
    private final String jobId;
    private final StageExecutorResult result;

    public SubmitJobResult(String jobId, StageExecutorResult result) {
      this.jobId = jobId;
      if (jobId != null) {
        this.result = StageExecutorResult.submitted();
      } else {
        this.result = StageExecutorResult.executionError();
      }
      // Preserve log and attributes.
      this.result.stageLog(result).attributes(result);
    }
  }

  /**
   * Submits the async job.
   *
   * @return the job id if successful.
   */
  protected abstract SubmitJobResult submitJob();

  /** Ends the async job. */
  protected void endJob() {}

  /** Terminates the asynchronous job. */
  protected abstract void terminateJob();

  @Override
  public StageExecutorResult execute() {
    if (jobId == null) {
      execStartTime = ZonedDateTime.now();
      return submit();
    }

    if (stageExecutorResult != null) {
      // Async job has already completed.
      return stageExecutorResult;
    } else {
      poll();
      if (stageExecutorResult != null) {
        return stageExecutorResult;
      }
      return StageExecutorResult.active();
    }
  }

  private StageExecutorResult submit() {
    logContext(log.atInfo()).log("Submitting " + executorName + " job");

    ZonedDateTime submitStartTime = ZonedDateTime.now();
    AtomicReference<StageExecutorResult> result = new AtomicReference<>();

    // Unexpected exceptions are logged as internal errors and the stage execution
    // is considered failed.
    internalErrorHandler.execute(
        () -> {
          prepareJob();
          SubmitJobResult submitJobResult = submitJob();
          result.set(submitJobResult.getResult());
          // Set the job id.
          jobId = submitJobResult.getJobId();
          if (jobId != null) {
            logContext(log.atInfo()).log("Submitted " + executorName + " job " + jobId);
          } else {
            String stageLog = submitJobResult.result.stageLog();
            logContext(log.atSevere())
                .log(
                    "Failed to submit "
                        + executorName
                        + " job"
                        + (stageLog != null ? "\n" + stageLog : ""));
          }
        },
        (ex) -> result.set(StageExecutorResult.internalError().stageLog(ex)));

    if (stageMetrics != null) {
      stageMetrics.executor().endSubmit(submitStartTime);
    }

    return result.get();
  }

  private void poll() {
    if (stageExecutorResult != null) {
      // Async job has already completed.
      return;
    }

    // Unexpected exceptions are logged as internal errors and the stage execution
    // is considered as failed.
    internalErrorHandler.execute(
        () -> {
          StageExecutorResult result =
              getDescribeJobs()
                  .getResult(getRequestContext(), getExecutorParams().getPermanentErrors());
          if (result.isCompleted()) {
            logContext(log.atInfo())
                .log(
                    "Completed "
                        + executorName
                        + " job "
                        + jobId
                        + " with state "
                        + result.state().name());
            stageExecutorResult = result;
            execEndTime = ZonedDateTime.now();
          }

          if (stageExecutorResult != null) {
            endJob();
          }
        },
        ex -> {
          stageExecutorResult = StageExecutorResult.internalError().stageLog(ex);
          execEndTime = ZonedDateTime.now();
        });
  }

  @Override
  public final void terminate() {
    logContext(log.atInfo()).log("Terminating " + executorName + " job " + jobId);
    RetryableExternalAction.execute(
        () -> {
          terminateJob();
          return null;
        });
    getDescribeJobs().removeRequest(getRequestContext());
  }

  private FluentLogger.Api logContext(FluentLogger.Api log) {
    return log.with(LogKey.PIPELINE_NAME, pipelineName)
        .with(LogKey.PROCESS_ID, processId)
        .with(LogKey.EXECUTOR_NAME, executorName)
        .with(LogKey.JOB_ID, jobId);
  }
}
