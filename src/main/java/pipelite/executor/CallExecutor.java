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
import pipelite.stage.Stage;
import pipelite.stage.executor.StageExecutorCall;
import pipelite.stage.executor.StageExecutorResult;
import pipelite.stage.executor.StageExecutorResultType;
import pipelite.stage.parameters.ExecutorParameters;

public class CallExecutor extends AbstractExecutor<ExecutorParameters> {

  private final StageExecutorCall call;
  private final Deque<StageExecutorResultType> resultTypes;

  /**
   * Forwards execution to the provided call.
   *
   * @param call the execution call
   */
  public CallExecutor(StageExecutorCall call) {
    this.call = call;
    this.resultTypes = null;
  }

  /**
   * When executed returns an execution result with the given result type.
   *
   * @param resultType the result type
   */
  public CallExecutor(StageExecutorResultType resultType) {
    this.call = (pipelineName, processId, stage) -> new StageExecutorResult(resultType);
    this.resultTypes = null;
  }

  /**
   * When executed returns an execution result of the given result types. Once all result types have
   * been returned returns null.
   *
   * @param resultTypes the result types
   */
  public CallExecutor(Collection<StageExecutorResultType> resultTypes) {
    this.call = null;
    this.resultTypes = new ArrayDeque<>(resultTypes);
  }

  @Override
  public StageExecutorResult execute(String pipelineName, String processId, Stage stage) {
    if (this.call != null) {
      return call.execute(pipelineName, processId, stage);
    }

    if (this.resultTypes == null) {
      return null;
    }
    return new StageExecutorResult(resultTypes.pollFirst());
  }
}
