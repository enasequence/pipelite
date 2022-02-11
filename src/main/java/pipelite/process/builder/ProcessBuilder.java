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
import java.util.stream.Collectors;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.SimpleDirectedGraph;
import pipelite.exception.PipeliteProcessStagesException;
import pipelite.process.Process;
import pipelite.stage.Stage;

public class ProcessBuilder {

  private final String processId;

  private final List<ProcessBuilderHelper.AddedStage> addedStages = new ArrayList<>();

  void addStage(Stage stage, List<String> dependsOnStageNames) {
    addedStages.add(new ProcessBuilderHelper.AddedStage(stage, dependsOnStageNames));
  }

  public ProcessBuilder(String processId) {
    this.processId = processId;
  }

  public String getProcessId() {
    return processId;
  }

  public StageBuilder execute(String stageName) {
    return new StageBuilder(this, stageName, Collections.emptyList());
  }

  public StageBuilder executeAfter(String stageName, String dependsOnStageName) {
    return new StageBuilder(this, stageName, Arrays.asList(dependsOnStageName));
  }

  public StageBuilder executeAfter(String stageName, List<String> dependsOnStageName) {
    return new StageBuilder(this, stageName, dependsOnStageName);
  }

  public StageBuilder executeAfterPrevious(String stageName) {
    return new StageBuilder(this, stageName, Arrays.asList(lastAddedStage().stage.getStageName()));
  }

  public StageBuilder executeAfterFirst(String stageName) {
    return new StageBuilder(this, stageName, Arrays.asList(firstAddedStage().stage.getStageName()));
  }

  public Process build() {
    // Build the graph

    if (addedStages.isEmpty()) {
      throw new PipeliteProcessStagesException("Process has no stages");
    }

    HashSet<String> nonUniqueStageNames = ProcessBuilderHelper.nonUniqueStageNames(addedStages);
    if (!nonUniqueStageNames.isEmpty()) {
      throw new PipeliteProcessStagesException(
          "Process has non-unique stage names: " + String.join(",", nonUniqueStageNames));
    }

    HashSet<String> nonExistingStageNames = ProcessBuilderHelper.nonExistingStageNames(addedStages);
    if (!nonExistingStageNames.isEmpty()) {
      throw new PipeliteProcessStagesException(
          "Process has non-existing stage dependencies: "
              + String.join(",", nonExistingStageNames));
    }

    HashSet<String> dependsOnSelfStageNames =
        ProcessBuilderHelper.dependsOnSelfStageNames(addedStages);
    if (!dependsOnSelfStageNames.isEmpty()) {
      throw new PipeliteProcessStagesException(
          "Process has self-referencing stage dependencies: "
              + String.join(",", dependsOnSelfStageNames));
    }

    SimpleDirectedGraph<Stage, DefaultEdge> stageGraph =
        ProcessBuilderHelper.stageGraph(addedStages);

    Set<Stage> stageCycles = ProcessBuilderHelper.stageCycles(stageGraph);
    if (!stageCycles.isEmpty()) {
      throw new PipeliteProcessStagesException(
          "Process has cyclic stage dependencies: "
              + String.join(
                  ",",
                  stageCycles.stream().map(s -> s.getStageName()).collect(Collectors.toList())));
    }

    return new Process(processId, stageGraph);
  }

  private ProcessBuilderHelper.AddedStage lastAddedStage() {
    return addedStages.get(addedStages.size() - 1);
  }

  private ProcessBuilderHelper.AddedStage firstAddedStage() {
    return addedStages.get(0);
  }
}
