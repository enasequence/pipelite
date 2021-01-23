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
package pipelite.process;

import java.util.HashSet;
import java.util.List;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.extern.flogger.Flogger;
import pipelite.entity.ProcessEntity;
import pipelite.stage.Stage;

@Flogger
@Data
public class Process {
  private final String processId;
  @EqualsAndHashCode.Exclude private final List<Stage> stages;
  private ProcessEntity processEntity;

  public Process(String processId, List<Stage> stages) {
    this.processId = processId;
    this.stages = stages;

    if (processId == null) {
      throw new IllegalArgumentException("Missing process id");
    }
    if (stages == null || stages.isEmpty()) {
      throw new IllegalArgumentException("Missing stages");
    }
    HashSet<String> stageNames = new HashSet<>();
    for (Stage stage : stages) {
      if (stageNames.contains(stage.getStageName())) {
        throw new IllegalArgumentException("Duplicate stage name: " + stage.getStageName());
      }
      stageNames.add(stage.getStageName());
    }
  }
}
