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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Value;
import pipelite.stage.Stage;
import pipelite.stage.StageExecutionResult;
import pipelite.stage.StageExecutionResultType;

@Value
public class EmptySyncStageExecutor implements StageExecutor {

  private final StageExecutionResultType resultType;

  @JsonCreator
  public EmptySyncStageExecutor(@JsonProperty("resultType") StageExecutionResultType resultType) {
    this.resultType = resultType;
  }

  @Override
  public StageExecutionResult execute(String pipelineName, String processId, Stage stage) {
    return new StageExecutionResult(resultType);
  }
}
