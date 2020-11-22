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

import java.util.ArrayList;
import java.util.List;
import pipelite.process.Process;
import pipelite.stage.Stage;
import pipelite.stage.StageParameters;

public class ProcessBuilder {

  final String processId;
  final List<Stage> stages = new ArrayList<>();

  public ProcessBuilder(String processId) {
    this.processId = processId;
  }

  public StageBuilder execute(String stageName) {
    return new StageBuilder(this, stageName, null, StageParameters.builder().build());
  }

  public StageBuilder execute(String stageName, StageParameters stageParameters) {
    return new StageBuilder(this, stageName, null, stageParameters);
  }

  public StageBuilder executeAfter(String stageName, String dependsOnStageName) {
    return new StageBuilder(this, stageName, dependsOnStageName, StageParameters.builder().build());
  }

  public StageBuilder executeAfter(
      String stageName, String dependsOnStageName, StageParameters stageParameters) {
    return new StageBuilder(this, stageName, dependsOnStageName, stageParameters);
  }

  public StageBuilder executeAfterPrevious(String stageName) {
    return new StageBuilder(
        this, stageName, lastStage().getStageName(), StageParameters.builder().build());
  }

  public StageBuilder executeAfterPrevious(String stageName, StageParameters stageParameters) {
    return new StageBuilder(this, stageName, lastStage().getStageName(), stageParameters);
  }

  public StageBuilder executeAfterFirst(String stageName) {
    return new StageBuilder(
        this, stageName, firstStage().getStageName(), StageParameters.builder().build());
  }

  public StageBuilder executeAfterFirst(String stageName, StageParameters stageParameters) {
    return new StageBuilder(this, stageName, firstStage().getStageName(), stageParameters);
  }

  public Process build() {
    return Process.builder().processId(this.processId).stages(this.stages).build();
  }

  private Stage lastStage() {
    return this.stages.get(this.stages.size() - 1);
  }

  private Stage firstStage() {
    return this.stages.get(0);
  }
}
