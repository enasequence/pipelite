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

public class DependencyResolver {

  private final List<ProcessLauncher.StageAndStageEntity> stageAndStageEntities;

  public DependencyResolver(List<ProcessLauncher.StageAndStageEntity> stageAndStageEntities) {
    this.stageAndStageEntities = stageAndStageEntities;
  }

  public List<ProcessLauncher.StageAndStageEntity> getDependentStages(
      ProcessLauncher.StageAndStageEntity from) {
    List<ProcessLauncher.StageAndStageEntity> dependentStages = new ArrayList<>();
    getDependentStages(dependentStages, from, false);
    return dependentStages;
  }

  private void getDependentStages(
      List<ProcessLauncher.StageAndStageEntity> dependentStages,
      ProcessLauncher.StageAndStageEntity from,
      boolean include) {

    for (ProcessLauncher.StageAndStageEntity stage : stageAndStageEntities) {
      if (stage.getStage().equals(from)) {
        continue;
      }

      Stage dependsOn = stage.getStage().getDependsOn();
      if (dependsOn != null && dependsOn.getStageName().equals(from.getStage().getStageName())) {
        getDependentStages(dependentStages, stage, true);
      }
    }

    if (include) {
      dependentStages.add(from);
    }
  }

  public List<ProcessLauncher.StageAndStageEntity> getExecutableStages() {
    List<ProcessLauncher.StageAndStageEntity> runnableStages = new ArrayList<>();
    for (ProcessLauncher.StageAndStageEntity stageAndStageEntity : stageAndStageEntities) {
      Stage stage = stageAndStageEntity.getStage();
      StageEntity stageEntity = stageAndStageEntity.getStageEntity();

      if (isDependsOnStageCompleted(stage)) {
        switch (stageEntity.getResultType()) {
          case NEW:
          case ACTIVE:
            runnableStages.add(stageAndStageEntity);
            break;
          case SUCCESS:
            break;
          case ERROR:
            {
              Integer executionCount = stageEntity.getExecutionCount();
              int maximumRetries = ProcessLauncher.getMaximumRetries(stage);
              int immediateRetries = ProcessLauncher.getImmediateRetries(stage);

              if (executionCount != null
                  && executionCount < maximumRetries
                  && stageAndStageEntity.immediateExecutionCount < immediateRetries) {
                runnableStages.add(stageAndStageEntity);
              }
            }
        }
      }
    }

    return runnableStages;
  }

  private boolean isDependsOnStageCompleted(Stage stage) {
    Stage dependsOnStage = stage.getDependsOn();
    if (dependsOnStage == null) {
      return true;
    }

    return stageAndStageEntities.stream()
            .filter(a -> a.getStage().equals(dependsOnStage))
            .findFirst()
            .get()
            .getStageEntity()
            .getResultType()
        == StageExecutionResultType.SUCCESS;
  }
}
