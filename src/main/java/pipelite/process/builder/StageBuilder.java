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

import java.util.Optional;
import pipelite.executor.StageExecutor;
import pipelite.stage.Stage;
import pipelite.stage.StageParameters;

public class StageBuilder {
  private final ProcessBuilder processBuilder;
  private final String stageName;
  private final String dependsOnStageName;
  private final StageParameters stageParameters;

  public StageBuilder(
      ProcessBuilder processBuilder,
      String stageName,
      String dependsOnStageName,
      StageParameters stageParameters) {
    this.processBuilder = processBuilder;
    this.stageName = stageName;
    this.dependsOnStageName = dependsOnStageName;
    this.stageParameters = stageParameters;
  }

  public ProcessBuilderWithDependsOn with(StageExecutor executor) {
    return addStage(executor);
  }

  public ProcessBuilderWithDependsOn withLocalCmdExecutor(String cmd) {
    return addStage(StageExecutor.createLocalCmdExecutor(cmd));
  }

  public ProcessBuilderWithDependsOn withSshCmdExecutor(String cmd) {
    return addStage(StageExecutor.createSshCmdExecutor(cmd));
  }

  public ProcessBuilderWithDependsOn withLsfLocalCmdExecutor(String cmd) {
    return addStage(StageExecutor.createLsfLocalCmdExecutor(cmd));
  }

  public ProcessBuilderWithDependsOn withLsfSshCmdExecutor(String cmd) {
    return addStage(StageExecutor.createLsfSshCmdExecutor(cmd));
  }

  private ProcessBuilderWithDependsOn addStage(StageExecutor executor) {
    Stage dependsOn = null;
    if (dependsOnStageName != null) {
      Optional<Stage> dependsOnOptional =
          processBuilder.stages.stream()
              .filter(stage -> stage.getStageName().equals(dependsOnStageName))
              .findFirst();

      if (!dependsOnOptional.isPresent()) {
        throw new IllegalArgumentException("Unknown stage dependency: " + dependsOnStageName);
      }
      dependsOn = dependsOnOptional.get();
    }

    processBuilder.stages.add(
        Stage.builder()
            .pipelineName(processBuilder.pipelineName)
            .processId(processBuilder.processId)
            .stageName(stageName)
            .executor(executor)
            .dependsOn(dependsOn)
            .stageParameters(stageParameters)
            .build());
    return new ProcessBuilderWithDependsOn(processBuilder);
  }
}
