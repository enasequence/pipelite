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
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import pipelite.executor.StageExecutor;
import pipelite.executor.StageExecutorParameters;
import pipelite.stage.Stage;
import pipelite.stage.StageExecutionResult;

public class StageBuilder {
  private final ProcessBuilder processBuilder;
  private final String stageName;
  private final List<String> dependsOnStageNames = new ArrayList<>();
  private final StageExecutorParameters executorParams;

  public StageBuilder(
      ProcessBuilder processBuilder,
      String stageName,
      Collection<String> dependsOnStageNames,
      StageExecutorParameters executorParams) {
    this.processBuilder = processBuilder;
    this.stageName = stageName;
    this.dependsOnStageNames.addAll(dependsOnStageNames);
    this.executorParams = executorParams;
  }

  public ProcessBuilder with(StageExecutor executor) {
    return addStage(executor);
  }

  public ProcessBuilder withLocalCmdExecutor(String cmd) {
    return addStage(StageExecutor.createLocalCmdExecutor(cmd));
  }

  public ProcessBuilder withSshCmdExecutor(String cmd) {
    return addStage(StageExecutor.createSshCmdExecutor(cmd));
  }

  public ProcessBuilder withLsfLocalCmdExecutor(String cmd) {
    return addStage(StageExecutor.createLsfLocalCmdExecutor(cmd));
  }

  public ProcessBuilder withLsfSshCmdExecutor(String cmd) {
    return addStage(StageExecutor.createLsfSshCmdExecutor(cmd));
  }

  public ProcessBuilder withEmptyExecutor(StageExecutionResult result) {
    return addStage(emptyExecutor(result));
  }

  public static StageExecutor emptyExecutor(StageExecutionResult result) {
    return (pipelineName, processId, Stage) -> result;
  }

  private ProcessBuilder addStage(StageExecutor executor) {
    List<Stage> dependsOn = new ArrayList<>();
    for (String dependsOnStageName : dependsOnStageNames) {
      Optional<Stage> dependsOnOptional =
          processBuilder.stages.stream()
              .filter(stage -> stage.getStageName().equals(dependsOnStageName))
              .findFirst();

      if (!dependsOnOptional.isPresent()) {
        throw new IllegalArgumentException("Unknown stage dependency: " + dependsOnStageName);
      }
      dependsOn.add(dependsOnOptional.get());
    }

    processBuilder.stages.add(
        Stage.builder()
            .stageName(stageName)
            .executor(executor)
            .dependsOn(dependsOn)
            .executorParams(executorParams)
            .build());
    return processBuilder;
  }
}
