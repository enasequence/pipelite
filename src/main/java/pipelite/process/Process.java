/*
 * Copyright 2020-2022 EMBL - European Bioinformatics Institute
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package pipelite.process;

import java.util.Optional;
import java.util.Set;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.extern.flogger.Flogger;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.SimpleDirectedGraph;
import pipelite.entity.ProcessEntity;
import pipelite.exception.PipeliteException;
import pipelite.stage.Stage;

@Flogger
@Data
public class Process {
  private final String processId;
  @EqualsAndHashCode.Exclude private final SimpleDirectedGraph<Stage, DefaultEdge> stageGraph;
  private ProcessEntity processEntity;

  public Process(String processId, SimpleDirectedGraph<Stage, DefaultEdge> stageGraph) {
    this.processId = processId;
    this.stageGraph = stageGraph;

    if (processId == null) {
      throw new PipeliteException("Missing process id");
    }
  }

  public Set<Stage> getStages() {
    return stageGraph.vertexSet();
  }

  public Optional<Stage> getStage(String stageName) {
    return getStages().stream().filter(s -> s.getStageName().equals(stageName)).findFirst();
  }
}
