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
package pipelite.process.builder;

import pipelite.process.Process;
import pipelite.stage.Stage;
import pipelite.stage.StageParameters;

public class ProcessBuilderWithDependsOn {

  private final ProcessBuilder processBuilder;

  public ProcessBuilderWithDependsOn(ProcessBuilder processBuilder) {
    this.processBuilder = processBuilder;
  }

  public StageBuilder execute(String stageName) {
    return new StageBuilder(processBuilder, stageName, null, StageParameters.builder().build());
  }

  public StageBuilder executeAfter(String stageName, String dependsOnStageName) {
    return new StageBuilder(
        processBuilder, stageName, dependsOnStageName, StageParameters.builder().build());
  }

  public StageBuilder executeAfter(
      String stageName, String dependsOnStageName, StageParameters stageParameters) {
    return new StageBuilder(processBuilder, stageName, dependsOnStageName, stageParameters);
  }

  public StageBuilder executeAfterPrevious(String stageName) {
    return new StageBuilder(
        processBuilder, stageName, lastStage().getStageName(), StageParameters.builder().build());
  }

  public StageBuilder executeAfterPrevious(String stageName, StageParameters stageParameters) {
    return new StageBuilder(processBuilder, stageName, lastStage().getStageName(), stageParameters);
  }

  public StageBuilder executeAfterFirst(String stageName) {
    return new StageBuilder(
        processBuilder, stageName, firstStage().getStageName(), StageParameters.builder().build());
  }

  public StageBuilder executeAfterFirst(String stageName, StageParameters stageParameters) {
    return new StageBuilder(
        processBuilder, stageName, firstStage().getStageName(), stageParameters);
  }

  public Process build() {
    return Process.builder()
        .pipelineName(processBuilder.pipelineName)
        .processId(processBuilder.processId)
        .stages(processBuilder.stages)
        .build();
  }

  private Stage lastStage() {
    return processBuilder.stages.get(processBuilder.stages.size() - 1);
  }

  private Stage firstStage() {
    return processBuilder.stages.get(0);
  }
}
