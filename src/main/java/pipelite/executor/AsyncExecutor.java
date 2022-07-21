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
package pipelite.executor;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.flogger.FluentLogger;
import java.time.ZonedDateTime;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import lombok.Getter;
import lombok.Setter;
import lombok.Value;
import lombok.extern.flogger.Flogger;
import pipelite.error.InternalErrorHandler;
import pipelite.exception.PipeliteException;
import pipelite.executor.describe.DescribeJobs;
import pipelite.executor.describe.context.DefaultExecutorContext;
import pipelite.executor.describe.context.DefaultRequestContext;
import pipelite.log.LogKey;
import pipelite.metrics.StageMetrics;
import pipelite.retryable.RetryableExternalAction;
import pipelite.service.InternalErrorService;
import pipelite.service.PipeliteServices;
import pipelite.stage.Stage;
import pipelite.stage.executor.StageExecutorResult;
import pipelite.stage.executor.StageExecutorResultCallback;
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

  @JsonIgnore private DescribeJobs<RequestContext, ExecutorContext> describeJobs;
  @JsonIgnore private AtomicReference<RequestContext> requestContext = new AtomicReference<>();

  @JsonIgnore private String pipelineName;
  @JsonIgnore private String processId;
  @JsonIgnore private String stageName;

  /** The async job completion result. */
  @JsonIgnore private StageExecutorResult jobCompletedResult;
  /** The async job completion time. */
  @JsonIgnore private ZonedDateTime jobCompletedTime;

  @JsonIgnore private InternalErrorService internalErrorService;
  @JsonIgnore private StageMetrics stageMetrics;

  @JsonIgnore private ReentrantLock submitLock = new ReentrantLock();
  @JsonIgnore private ZonedDateTime submitStartTime;

  @Override
  public void prepareExecution(
      PipeliteServices pipeliteServices, String pipelineName, String processId, Stage stage) {
    super.prepareExecution(pipeliteServices, pipelineName, processId, stage);
    this.describeJobs = prepareDescribeJobs(pipeliteServices);
    this.pipelineName = pipelineName;
    this.processId = processId;
    this.stageName = stage.getStageName();
    if (pipeliteServices != null) {
      this.internalErrorService = pipeliteServices.internalError();
      this.stageMetrics = pipeliteServices.metrics().process(pipelineName).stage(stageName);
    }
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

    public SubmitJobResult(String jobId) {
      this(jobId, null);
    }

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
  public void execute(StageExecutorResultCallback resultCallback) {
    if (jobId == null) {
      if (submitLock.tryLock()) {
        logContext(log.atInfo()).log("Submitting async job");
        submitStartTime = ZonedDateTime.now();
        submit(resultCallback);
      } else {
        logContext(log.atFine()).log("Waiting for async job submission to complete");
      }
    } else {
      poll(resultCallback);
    }
  }

  private void submit(StageExecutorResultCallback resultCallback) {
    try {
      InternalErrorHandler internalErrorHandler =
          new InternalErrorHandler(internalErrorService, pipelineName, processId, stageName, this);

      ZonedDateTime submitStartTime = ZonedDateTime.now();

      AtomicReference<SubmitJobResult> submitJobResult = new AtomicReference<>();
      internalErrorHandler.execute(
          () -> {
            prepareJob();
            submitJobResult.set(submitJob());
            // Set the job id.
            jobId = submitJobResult.get().getJobId();
          });

      internalErrorHandler.execute(
          () -> {
            if (jobId != null) {
              logContext(log.atInfo())
                  .log(
                      "Submitted async job "
                          + jobId
                          + " pipeline "
                          + pipelineName
                          + " process "
                          + processId
                          + " stage "
                          + stageName);
            } else {
              logContext(log.atSevere())
                  .log(
                      "Failed to submit async job"
                          + " pipeline "
                          + pipelineName
                          + " process "
                          + processId
                          + " stage "
                          + stageName);
            }
            resultCallback.accept(submitJobResult.get().getResult());

            if (stageMetrics != null) {
              stageMetrics.executor().endSubmit(submitStartTime);
            }
          });
    } finally {
      submitLock.unlock();
    }
  }

  private void poll(StageExecutorResultCallback resultCallback) {
    InternalErrorHandler internalErrorHandler =
        new InternalErrorHandler(internalErrorService, pipelineName, processId, stageName, this);

    internalErrorHandler.execute(
        () -> {
          if (jobCompletedResult == null) {
            internalErrorHandler.execute(
                () -> {
                  StageExecutorResult result =
                      getDescribeJobs()
                          .getResult(getRequestContext(), getExecutorParams().getPermanentErrors());
                  if (!result.isActive()) {
                    // Async job has completed.
                    logContext(log.atInfo())
                        .log(
                            "Completed async job with job id "
                                + getJobId()
                                + " and state "
                                + result.state().name());
                    jobCompletedResult = result;
                    jobCompletedTime = ZonedDateTime.now();
                  }
                });
          }
          if (jobCompletedResult != null) {
            internalErrorHandler.execute(() -> endJob());
            internalErrorHandler.execute(() -> resultCallback.accept(jobCompletedResult));
          }
        },
        ex -> {
          jobCompletedResult = StageExecutorResult.internalError().stageLog(ex);
          internalErrorHandler.execute(() -> resultCallback.accept(jobCompletedResult));
        });
  }

  @Override
  public final void terminate() {
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
        .with(LogKey.STAGE_NAME, stageName);
  }
}
