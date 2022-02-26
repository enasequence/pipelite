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
import lombok.Getter;
import lombok.Setter;
import pipelite.exception.PipeliteException;
import pipelite.executor.describe.cache.DescribeJobsCache;
import pipelite.executor.state.AsyncExecutorState;
import pipelite.service.StageService;
import pipelite.stage.executor.StageExecutorRequest;
import pipelite.stage.executor.StageExecutorResult;
import pipelite.stage.parameters.ExecutorParameters;

/** Executes a stage. Must be serializable to json. */
@Getter
@Setter
public abstract class AbstractAsyncExecutor<
        T extends ExecutorParameters, D extends DescribeJobsCache>
    extends AbstractExecutor<T> {

  /**
   * Asynchronous executor state. Serialize in database to continue execution after service restart.
   * State is initialized in prepareAsyncExecute for backward compatibility between 1.4.* and
   * previous versions.
   */
  private AsyncExecutorState state;

  /**
   * Asynchronous executor job id. Serialize in database to continue execution after service
   * restart.
   */
  private String jobId;

  @JsonIgnore private D describeJobsCache;

  /**
   * Prepares stage executor for asynchronous execution.
   *
   * @param stageService the stage service
   */
  public void prepareAsyncExecute(StageService stageService) {
    this.describeJobsCache = initDescribeJobsCache(stageService);
    // For backward compatibility between 1.4.* and previous versions. Versions older than 1.4.*
    // do not serialize state. Set state to POLL if job id has been set.
    if (state == null) {
      if (jobId != null) {
        state = AsyncExecutorState.POLL;
      } else {
        state = AsyncExecutorState.SUBMIT;
      }
    }
  }

  protected abstract D initDescribeJobsCache(StageService stageService);

  public D getDescribeJobsCache() {
    return describeJobsCache;
  }

  protected final void prepareSubmit(StageExecutorRequest request) {
    // Reset job id to allow execution retry.
    jobId = null;
    prepareAsyncSubmit(request);
  }

  protected abstract void prepareAsyncSubmit(StageExecutorRequest request);

  protected abstract StageExecutorResult submit(StageExecutorRequest request);

  protected abstract StageExecutorResult poll(StageExecutorRequest request);

  @Override
  public final StageExecutorResult execute(StageExecutorRequest request) {
    if (state == AsyncExecutorState.SUBMIT) {
      prepareSubmit(request);
      StageExecutorResult result = submit(request);
      if (result.isError()) {
        return result;
      }
      if (!result.isSubmitted()) {
        throw new PipeliteException(
            "Unexpected state during asynchronous submit: " + result.getExecutorState().name());
      }
      if (jobId == null) {
        throw new PipeliteException("Missing job id.");
      }
      state = AsyncExecutorState.POLL;
      // Asynchronous executor state is saved in database after submit by stage runner.
      return result;
    }
    if (state == AsyncExecutorState.POLL) {
      if (jobId == null) {
        throw new PipeliteException("Missing job id during asynchronous poll");
      }
      StageExecutorResult result = poll(request);
      if (result.isSubmitted()) {
        throw new PipeliteException(
            "Unexpected state during asynchronous poll: " + result.getExecutorState().name());
      }
      if (!result.isActive()) {
        // Prepare for next asynchronous execution.
        state = AsyncExecutorState.SUBMIT;
      }
      return result;
    }
    throw new PipeliteException("Missing state during asynchronous execution");
  }
}
