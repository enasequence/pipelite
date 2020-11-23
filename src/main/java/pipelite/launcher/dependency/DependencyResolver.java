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
package pipelite.launcher.dependency;

import java.util.ArrayList;
import java.util.List;
import pipelite.entity.StageEntity;
import pipelite.launcher.ProcessLauncher;
import pipelite.stage.Stage;
import pipelite.stage.StageExecutionResultType;

import static pipelite.stage.StageExecutionResultType.NEW;

public class DependencyResolver {

  private final List<ProcessLauncher.StageExecution> stageExecutions;

  public DependencyResolver(List<ProcessLauncher.StageExecution> stageExecutions) {
    this.stageExecutions = stageExecutions;
  }

  public List<ProcessLauncher.StageExecution> getDependentStages(
      ProcessLauncher.StageExecution from) {
    List<ProcessLauncher.StageExecution> dependentStages = new ArrayList<>();
    getDependentStages(dependentStages, from, false);
    return dependentStages;
  }

  private void getDependentStages(
      List<ProcessLauncher.StageExecution> dependentStages,
      ProcessLauncher.StageExecution from,
      boolean include) {

    for (ProcessLauncher.StageExecution stageExecution : stageExecutions) {
      Stage stage = stageExecution.getStage();
      if (stage.equals(from)) {
        continue;
      }

      Stage dependsOn = stage.getDependsOn();
      if (dependsOn != null && dependsOn.getStageName().equals(from.getStage().getStageName())) {
        getDependentStages(dependentStages, stageExecution, true);
      }
    }

    if (include) {
      dependentStages.add(from);
    }
  }

  public List<ProcessLauncher.StageExecution> getExecutableStages() {
    List<ProcessLauncher.StageExecution> executableStages = new ArrayList<>();
    for (ProcessLauncher.StageExecution stageExecution : stageExecutions) {
      Stage stage = stageExecution.getStage();
      StageEntity stageEntity = stageExecution.getStageEntity();

      if (isDependsOnStageCompleted(stage)) {
        StageExecutionResultType resultType = stageEntity.getResultType();
        if (resultType == null) {
          resultType = NEW;
        }
        switch (resultType) {
          case NEW:
          case ACTIVE:
            executableStages.add(stageExecution);
            break;
          case SUCCESS:
            break;
          case ERROR:
            {
              Integer executionCount = stageEntity.getExecutionCount();
              int maximumRetries = ProcessLauncher.getMaximumRetries(stage);
              int immediateRetries = ProcessLauncher.getImmediateRetries(stage);

              if (executionCount != null
                  && executionCount <= maximumRetries
                  && stageExecution.getImmediateExecutionCount() <= immediateRetries) {
                executableStages.add(stageExecution);
              }
            }
        }
      }
    }

    return executableStages;
  }

  private boolean isDependsOnStageCompleted(Stage stage) {
    Stage dependsOnStage = stage.getDependsOn();
    if (dependsOnStage == null) {
      return true;
    }

    return stageExecutions.stream()
            .filter(e -> e.getStage().equals(dependsOnStage))
            .findFirst()
            .get()
            .getStageEntity()
            .getResultType()
        == StageExecutionResultType.SUCCESS;
  }
}
