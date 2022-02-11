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
import org.jgrapht.alg.cycle.CycleDetector;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.SimpleDirectedGraph;
import pipelite.stage.Stage;

public class ProcessBuilderHelper {
  public static class AddedStage {
    final Stage stage;
    final List<String> dependsOnStageNames;

    public AddedStage(Stage stage) {
      this.stage = stage;
      this.dependsOnStageNames = Collections.emptyList();
    }

    public AddedStage(Stage stage, List<String> dependsOnStageNames) {
      this.stage = stage;
      this.dependsOnStageNames =
          dependsOnStageNames != null ? dependsOnStageNames : Collections.emptyList();
    }

    boolean isStageName(String stageName) {
      return stage.getStageName().equals(stageName);
    }
  }

  private static Optional<ProcessBuilderHelper.AddedStage> getStage(
      Collection<AddedStage> addedStages, String stageName) {
    return addedStages.stream().filter(s -> s.isStageName(stageName)).findFirst();
  }

  public static HashSet<String> nonUniqueStageNames(List<AddedStage> addedStages) {
    HashSet<String> stageNames = new HashSet<>();
    HashSet<String> nonUniqueStageNames = new HashSet<>();
    addedStages.forEach(
        s -> {
          String stageName = s.stage.getStageName();
          if (stageNames.contains(stageName)) {
            nonUniqueStageNames.add(stageName);
          }
          stageNames.add(stageName);
        });
    return nonUniqueStageNames;
  }

  public static HashSet<String> nonExistingStageNames(List<AddedStage> addedStages) {
    HashSet<String> nonExistingStageNames = new HashSet<>();
    addedStages.forEach(
        addedStage -> {
          for (String dependsOnStageName : addedStage.dependsOnStageNames) {
            Optional<AddedStage> dependsOnAddedStage = getStage(addedStages, dependsOnStageName);
            if (!dependsOnAddedStage.isPresent()) {
              nonExistingStageNames.add(dependsOnStageName);
            }
          }
        });
    return nonExistingStageNames;
  }

  public static SimpleDirectedGraph<Stage, DefaultEdge> stageGraph(
      Collection<AddedStage> addedStages) {
    SimpleDirectedGraph<Stage, DefaultEdge> stageGraph =
        new SimpleDirectedGraph<>(DefaultEdge.class);
    addedStages.forEach(s -> stageGraph.addVertex(s.stage));
    addedStages.forEach(
        addedStage ->
            addedStage.dependsOnStageNames.forEach(
                dependsOnStageName -> {
                  AddedStage dependsOnAddedStage = getStage(addedStages, dependsOnStageName).get();
                  stageGraph.addEdge(dependsOnAddedStage.stage, addedStage.stage);
                }));
    return stageGraph;
  }

  public static Set<Stage> stageCycles(SimpleDirectedGraph<Stage, DefaultEdge> stageGraph) {
    return (new CycleDetector<>(stageGraph)).findCycles();
  }
}
