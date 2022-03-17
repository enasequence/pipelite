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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.locks.ReentrantLock;
import lombok.Getter;
import lombok.Setter;
import lombok.Value;
import pipelite.exception.PipeliteException;
import pipelite.executor.describe.cache.DescribeJobsCache;
import pipelite.metrics.StageMetrics;
import pipelite.service.DescribeJobsCacheService;
import pipelite.stage.executor.StageExecutorRequest;
import pipelite.stage.executor.StageExecutorResult;
import pipelite.stage.executor.StageExecutorResultCallback;
import pipelite.stage.parameters.ExecutorParameters;

/** Executes a stage. Must be serializable to json. */
@Getter
@Setter
public abstract class AbstractAsyncExecutor<
        T extends ExecutorParameters, D extends DescribeJobsCache>
    extends AbstractExecutor<T> {

  /**
   * Asynchronous executor job id. Serialize in database to continue execution after service
   * restart.
   */
  private String jobId;

  @JsonIgnore private ExecutorService submitExecutorService;
  @JsonIgnore private D describeJobsCache;
  @JsonIgnore private StageMetrics stageMetrics;

  @JsonIgnore private ReentrantLock submitLock = new ReentrantLock();

  /** Prepares stage executor for asynchronous execution. */
  public void prepareExecution(
      ExecutorService submitExecutorService,
      DescribeJobsCacheService describeJobsCacheService,
      StageMetrics stageMetrics) {
    this.submitExecutorService = submitExecutorService;
    this.describeJobsCache = initDescribeJobsCache(describeJobsCacheService);
    this.stageMetrics = stageMetrics;
  }

  protected abstract D initDescribeJobsCache(DescribeJobsCacheService describeJobsCacheService);

  public D getDescribeJobsCache() {
    return describeJobsCache;
  }

  protected void prepareSubmit(StageExecutorRequest request) {}

  @Value
  protected static class SubmitResult {
    private final String jobId;
    private final StageExecutorResult result;
  }

  protected abstract SubmitResult submit(StageExecutorRequest request);

  protected abstract StageExecutorResult poll(StageExecutorRequest request);

  @Override
  public void execute(StageExecutorRequest request, StageExecutorResultCallback resultCallback) {
    if (jobId == null) {
      if (submitLock.tryLock()) {
        submit(request, resultCallback);
      }
    } else {
      poll(request, resultCallback);
    }
  }

  private void submit(StageExecutorRequest request, StageExecutorResultCallback resultCallback) {
    submitExecutorService.submit(
        () -> {
          try {
            ZonedDateTime submitStartTime = ZonedDateTime.now();
            prepareSubmit(request);
            SubmitResult submitResult = submit(request);
            jobId = submitResult.getJobId();
            StageExecutorResult result = submitResult.getResult();

            if (stageMetrics != null) {
              stageMetrics
                  .getAsyncSubmitTimer()
                  .record(Duration.between(submitStartTime, ZonedDateTime.now()));
              stageMetrics.endAsyncSubmit();
            }

            if (!result.isError()) {
              if (!result.isSubmitted()) {
                result =
                    StageExecutorResult.internalError(
                        new PipeliteException(
                            "Unexpected state after asynchronous submit: "
                                + result.getExecutorState().name()));
              } else if (jobId == null) {
                result =
                    StageExecutorResult.internalError(
                        new PipeliteException("Missing job id after asynchronous submit"));
              }
            }
            resultCallback.accept(result);
          } catch (Exception ex) {
            StageExecutorResult result = StageExecutorResult.internalError(ex);
            resultCallback.accept(result);
          } finally {
            submitLock.unlock();
          }
        });
  }

  private void poll(StageExecutorRequest request, StageExecutorResultCallback resultCallback) {
    try {
      StageExecutorResult result = poll(request);
      if (result.isSubmitted()) {
        result =
            StageExecutorResult.internalError(
                new PipeliteException(
                    "Unexpected state during asynchronous poll: "
                        + result.getExecutorState().name()));
      }
      resultCallback.accept(result);
    } catch (Exception ex) {
      StageExecutorResult result = StageExecutorResult.internalError(ex);
      resultCallback.accept(result);
    }
  }
}
