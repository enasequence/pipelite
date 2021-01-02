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

import java.util.*;

import pipelite.exception.PipeliteException;
import pipelite.process.Process;
import pipelite.stage.Stage;
import pipelite.stage.parameters.ExecutorParameters;

public class ProcessBuilder {

  final String processId;
  final List<Stage> stages = new ArrayList<>();

  public ProcessBuilder(String processId) {
    this.processId = processId;
  }

  public String getProcessId() {
    return processId;
  }

  public StageBuilder execute(String stageName) {
    return new StageBuilder(this, stageName, Collections.emptyList());
  }

  public StageBuilder executeAfter(
      String stageName, String dependsOnStageName) {
    return new StageBuilder(this, stageName, Arrays.asList(dependsOnStageName));
  }

  public StageBuilder executeAfter(
      String stageName, List<String> dependsOnStageName) {
    return new StageBuilder(this, stageName, dependsOnStageName);
  }

  public StageBuilder executeAfterPrevious(
      String stageName) {
    return new StageBuilder(
        this, stageName, Arrays.asList(lastStage().getStageName()));
  }

  public StageBuilder executeAfterFirst(
      String stageName) {
    return new StageBuilder(
        this, stageName, Arrays.asList(firstStage().getStageName()));
  }

  public Process build() {
    return new Process(processId, this.stages);
  }

  private Stage lastStage() {
    return this.stages.get(this.stages.size() - 1);
  }

  private Stage firstStage() {
    return this.stages.get(0);
  }
}
