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

import java.time.Duration;
import java.util.function.Function;
import lombok.Getter;
import org.springframework.util.Assert;
import pipelite.stage.executor.StageExecutorRequest;
import pipelite.stage.executor.StageExecutorResult;
import pipelite.stage.executor.StageExecutorResultCallback;
import pipelite.stage.parameters.ExecutorParameters;
import pipelite.time.Time;

/** Synchronous test executor. */
@Getter
public class SyncTestExecutor extends SyncExecutor<ExecutorParameters> {

  private final Function<StageExecutorRequest, StageExecutorResult> callback;
  private final Duration executionTime;

  public SyncTestExecutor(
      Function<StageExecutorRequest, StageExecutorResult> callback, Duration executionTime) {
    Assert.notNull(callback, "Missing callback");
    this.callback = callback;
    this.executionTime = executionTime;
  }

  @Override
  public void execute(StageExecutorRequest request, StageExecutorResultCallback resultCallback) {
    if (executionTime != null) {
      Time.wait(executionTime);
    }
    resultCallback.accept(callback.apply(request));
  }

  @Override
  public void terminate() {}
}
