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
import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.concurrent.locks.ReentrantLock;
import lombok.Getter;
import lombok.Setter;
import lombok.Value;
import lombok.extern.flogger.Flogger;
import pipelite.exception.PipeliteSubmitException;
import pipelite.executor.describe.cache.DescribeJobsCache;
import pipelite.log.LogKey;
import pipelite.metrics.StageMetrics;
import pipelite.service.DescribeJobsCacheService;
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
public abstract class AsyncExecutor<T extends ExecutorParameters, D extends DescribeJobsCache>
    extends AbstractExecutor<T> {

  /**
   * Asynchronous executor job id. Serialize in database to continue execution after service
   * restart.
   */
  private String jobId;

  @JsonIgnore private String pipelineName;
  @JsonIgnore private String processId;
  @JsonIgnore private String stageName;
  @JsonIgnore private D describeJobsCache;
  @JsonIgnore private PollJobResult pollJobResult;

  @JsonIgnore private InternalErrorService internalErrorService;
  @JsonIgnore private StageMetrics stageMetrics;

  @JsonIgnore private ReentrantLock submitLock = new ReentrantLock();
  @JsonIgnore private ZonedDateTime submitStartTime;

  /** Prepares stage executor for asynchronous execution. */
  public void prepareExecution(
      PipeliteServices pipeliteServices, String pipelineName, String processId, Stage stage) {
    super.prepareExecution(pipeliteServices, pipelineName, processId, stage);
    this.pipelineName = pipelineName;
    this.processId = processId;
    this.stageName = stage.getStageName();
    if (pipeliteServices != null) {
      this.internalErrorService = pipeliteServices.internalError();
      this.describeJobsCache = initDescribeJobsCache(pipeliteServices.jobs());
      this.stageMetrics = pipeliteServices.metrics().process(pipelineName).stage(stageName);
    }
  }

  protected abstract D initDescribeJobsCache(DescribeJobsCacheService describeJobsCacheService);

  public D getDescribeJobsCache() {
    return describeJobsCache;
  }

  @Value
  protected static class SubmitJobResult {
    private final String jobId;
    private final StageExecutorResult result;
  }

  @Value
  protected static class PollJobResult {
    private final ZonedDateTime endTime = ZonedDateTime.now();
    private final StageExecutorResult result;
  }

  /** Prepares the async job. */
  protected void prepareJob() {}

  /** Submits the async job. */
  protected abstract SubmitJobResult submitJob();

  /** Polls the async job. */
  protected abstract StageExecutorResult pollJob();

  /**
   * Ends the async job.
   *
   * @return true if the async job execution is finished.
   */
  protected abstract boolean endJob(PollJobResult pollJobResult);

  @Override
  public void execute(StageExecutorResultCallback resultCallback) {
    if (jobId == null) {
      if (submitLock.tryLock()) {
        log.atInfo()
            .with(LogKey.PIPELINE_NAME, pipelineName)
            .with(LogKey.PROCESS_ID, processId)
            .with(LogKey.STAGE_NAME, stageName)
            .log("Submitting async job");
        submitStartTime = ZonedDateTime.now();
        submit(resultCallback);
      } else {
        log.atFine()
            .with(LogKey.PIPELINE_NAME, pipelineName)
            .with(LogKey.PROCESS_ID, processId)
            .with(LogKey.STAGE_NAME, stageName)
            .log("Waiting for async job submission to complete");
      }
    } else {
      poll(resultCallback);
    }
  }

  @Override
  @JsonIgnore
  public boolean isSubmitted() {
    return jobId != null;
  }

  private void submit(StageExecutorResultCallback resultCallback) {
    try {
      ZonedDateTime submitStartTime = ZonedDateTime.now();
      prepareJob();
      SubmitJobResult submitJobResult = submitJob();
      jobId = submitJobResult.getJobId();
      StageExecutorResult result = submitJobResult.getResult();

      if (stageMetrics != null) {
        stageMetrics.executor().endSubmit(submitStartTime);
      }

      PipeliteSubmitException submitException = null;
      if (result.isError()) {
        submitException = submitException(result.getStageLog());
      } else if (!result.isSubmitted()) {
        submitException = submitException("unexpected state " + result.getExecutorState().name());
        result = StageExecutorResult.internalError(submitException);
      } else if (jobId == null) {
        submitException = submitException("missing job id");
        result = StageExecutorResult.internalError(submitException);
      }

      saveInternalError(submitException);
      resultCallback.accept(result);
    } catch (Exception ex) {
      StageExecutorResult result = StageExecutorResult.internalError(ex);
      resultCallback.accept(result);
    } finally {
      Duration submitDuration = Duration.between(submitStartTime, ZonedDateTime.now());
      log.atInfo()
          .with(LogKey.PIPELINE_NAME, pipelineName)
          .with(LogKey.PROCESS_ID, processId)
          .with(LogKey.STAGE_NAME, stageName)
          .log(
              "Submitted async job with job id "
                  + getJobId()
                  + " in "
                  + submitDuration.toSeconds()
                  + " seconds");
      submitLock.unlock();
    }
  }

  private void poll(StageExecutorResultCallback resultCallback) {
    try {
      if (pollJobResult == null) {
        StageExecutorResult result = pollJob();
        if (!result.isActive()) {
          // Async job execution has completed.
          pollJobResult = new PollJobResult(result);
        }
      }
      if (pollJobResult != null) {
        if (endJob(pollJobResult)) {
          resultCallback.accept(pollJobResult.getResult());
        }
      }
    } catch (Exception ex) {
      StageExecutorResult result = StageExecutorResult.internalError(ex);
      resultCallback.accept(result);
    }
  }

  private PipeliteSubmitException submitException(String message) {
    return new PipeliteSubmitException(pipelineName, processId, stageName, message);
  }

  private void saveInternalError(Throwable throwable) {
    if (throwable != null && internalErrorService != null) {
      internalErrorService.saveInternalError(
          pipelineName, processId, stageName, AsyncExecutor.class, throwable);
    }
  }
}
