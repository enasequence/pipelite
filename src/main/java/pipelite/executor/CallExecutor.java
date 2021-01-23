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

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Deque;
import pipelite.stage.StageState;
import pipelite.stage.executor.StageExecutorCall;
import pipelite.stage.executor.StageExecutorRequest;
import pipelite.stage.executor.StageExecutorResult;
import pipelite.stage.parameters.ExecutorParameters;

public class CallExecutor extends AbstractExecutor<ExecutorParameters> {

  private final StageExecutorCall call;
  private final Deque<StageState> stageStates;

  /**
   * Forwards execution to the provided call.
   *
   * @param call the execution call
   */
  public CallExecutor(StageExecutorCall call) {
    this.call = call;
    this.stageStates = null;
  }

  /**
   * When executed returns an execution result with stage state.
   *
   * @param stageState the stage state
   */
  public CallExecutor(StageState stageState) {
    this.call = (request) -> new StageExecutorResult(stageState);
    this.stageStates = null;
  }

  /**
   * When executed returns an execution result of the given result types. Once all result types have
   * been returned returns null.
   *
   * @param stageStates the result types
   */
  public CallExecutor(Collection<StageState> stageStates) {
    this.call = null;
    this.stageStates = new ArrayDeque<>(stageStates);
  }

  @Override
  public StageExecutorResult execute(StageExecutorRequest request) {
    if (this.call != null) {
      return call.execute(request);
    }

    if (this.stageStates == null) {
      return null;
    }
    return new StageExecutorResult(stageStates.pollFirst());
  }

  @Override
  public void terminate() {}
}
