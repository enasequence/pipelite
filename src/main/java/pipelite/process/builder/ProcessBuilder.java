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
import pipelite.process.Process;
import pipelite.stage.Stage;
import pipelite.stage.executor.StageExecutorParameters;

public class ProcessBuilder {

  final String processId;
  final List<Stage> stages = new ArrayList<>();

  public ProcessBuilder(String processId) {
    this.processId = processId;
  }

  public StageBuilder execute(String stageName) {
    return new StageBuilder(
        this, stageName, Collections.emptyList(), StageExecutorParameters.builder().build());
  }

  public StageBuilder execute(String stageName, StageExecutorParameters executorParams) {
    return new StageBuilder(this, stageName, Collections.emptyList(), executorParams);
  }

  public StageBuilder executeAfter(String stageName, String dependsOnStageName) {
    return new StageBuilder(
        this,
        stageName,
        Arrays.asList(dependsOnStageName),
        StageExecutorParameters.builder().build());
  }

  public StageBuilder executeAfter(String stageName, List<String> dependsOnStageName) {
    return new StageBuilder(
        this, stageName, dependsOnStageName, StageExecutorParameters.builder().build());
  }

  public StageBuilder executeAfter(
      String stageName, String dependsOnStageName, StageExecutorParameters executorParams) {
    return new StageBuilder(this, stageName, Arrays.asList(dependsOnStageName), executorParams);
  }

  public StageBuilder executeAfter(
      String stageName, List<String> dependsOnStageName, StageExecutorParameters executorParams) {
    return new StageBuilder(this, stageName, dependsOnStageName, executorParams);
  }

  public StageBuilder executeAfterPrevious(String stageName) {
    return new StageBuilder(
        this,
        stageName,
        Arrays.asList(lastStage().getStageName()),
        StageExecutorParameters.builder().build());
  }

  public StageBuilder executeAfterPrevious(
      String stageName, StageExecutorParameters executorParams) {
    return new StageBuilder(
        this, stageName, Arrays.asList(lastStage().getStageName()), executorParams);
  }

  public StageBuilder executeAfterFirst(String stageName) {
    return new StageBuilder(
        this,
        stageName,
        Arrays.asList(firstStage().getStageName()),
        StageExecutorParameters.builder().build());
  }

  public StageBuilder executeAfterFirst(String stageName, StageExecutorParameters executorParams) {
    return new StageBuilder(
        this, stageName, Arrays.asList(firstStage().getStageName()), executorParams);
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
