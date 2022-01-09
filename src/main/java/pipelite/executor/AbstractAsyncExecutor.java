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
import pipelite.exception.PipeliteException;
import pipelite.executor.state.AsyncExecutorState;
import pipelite.stage.executor.StageExecutorDescribeJobsCache;
import pipelite.stage.executor.StageExecutorRequest;
import pipelite.stage.executor.StageExecutorResult;
import pipelite.stage.parameters.ExecutorParameters;

/** Executes a stage. Must be serializable to json. */
public abstract class AbstractAsyncExecutor<T extends ExecutorParameters>
    extends AbstractExecutor<T> {

  @JsonIgnore
  private AsyncExecutorState asyncExecutorState = AsyncExecutorState.SUBMIT;

  protected abstract void prepareSubmit(StageExecutorRequest request);

  protected abstract StageExecutorResult submit(StageExecutorRequest request);

  protected abstract StageExecutorResult poll(StageExecutorRequest request);

  @Override
  public final StageExecutorResult execute(StageExecutorRequest request) {
    if (asyncExecutorState == AsyncExecutorState.SUBMIT) {
      prepareSubmit(request);
      StageExecutorResult result = submit(request);
      if (result.isError()) {
        return result;
      }
      if (!result.isSubmitted()) {
        throw new PipeliteException(
            "Unexpected state during asynchronous submit: " + result.getExecutorState().name());
      }
      asyncExecutorState = AsyncExecutorState.POLL;
      return result;
    } else {
      StageExecutorResult result = poll(request);
      if (result.isSubmitted()) {
        throw new PipeliteException(
            "Unexpected state during asynchronous poll: " + result.getExecutorState().name());
      }
      if (!result.isActive()) {
        // Prepare for next asynchronous execution.
        asyncExecutorState = AsyncExecutorState.SUBMIT;
      }
      return result;
    }
  }
}
