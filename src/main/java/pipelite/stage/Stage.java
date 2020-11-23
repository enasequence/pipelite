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
package pipelite.stage;

import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.extern.flogger.Flogger;
import pipelite.executor.StageExecutor;

import java.util.List;

@Flogger
@Data
@Builder
public class Stage {
  private final String stageName;
  @EqualsAndHashCode.Exclude private StageExecutor executor;
  @EqualsAndHashCode.Exclude private final List<Stage> dependsOn;
  @EqualsAndHashCode.Exclude private final StageParameters stageParameters;

  public Stage(
      String stageName, StageExecutor executor, List<Stage> dependsOn, StageParameters stageParameters) {
    this.stageName = stageName;
    this.executor = executor;
    this.dependsOn = dependsOn;
    if (stageParameters != null) {
      this.stageParameters = stageParameters;
    } else {
      this.stageParameters = StageParameters.builder().build();
    }

    if (stageName == null || stageName.isEmpty()) {
      throw new IllegalArgumentException("Missing stage name");
    }
    if (executor == null) {
      throw new IllegalArgumentException("Missing executor");
    }
  }

  public StageExecutionResult execute(String pipelineName, String processId) {
    return executor.execute(pipelineName, processId, this);
  }
}
