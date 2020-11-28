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

  private DependencyResolver() {};

  public static List<Stage> getDependentStages(List<Stage> stages, Stage from) {
    List<Stage> dependentStages = new ArrayList<>();
    getDependentStages(stages, dependentStages, from, false);
    return dependentStages;
  }

  private static void getDependentStages(
      List<Stage> stages, List<Stage> dependentStages, Stage from, boolean include) {

    for (Stage stage : stages) {
      if (stage.equals(from)) {
        continue;
      }

      if (stage.getDependsOn() != null) {
        for (Stage dependsOn : stage.getDependsOn()) {
          if (dependsOn.getStageName().equals(from.getStageName())) {
            getDependentStages(stages, dependentStages, stage, true);
          }
        }
      }
    }

    if (include) {
      dependentStages.add(from);
    }
  }

  public static List<Stage> getDependsOnStages(List<Stage> stages, Stage from) {
    List<Stage> dependsOnStages = new ArrayList<>();
    getDependsOnStages(stages, dependsOnStages, from);
    return dependsOnStages;
  }

  private static void getDependsOnStages(
      List<Stage> stages, List<Stage> dependsOnStages, Stage from) {
    if (from.getDependsOn() != null) {
      for (Stage dependsOn : from.getDependsOn()) {
        for (Stage stage : stages) {
          if (stage.equals(dependsOn)) {
            if (!dependsOnStages.contains(stage)) {
              dependsOnStages.add(stage);
              getDependsOnStages(stages, dependsOnStages, stage);
            }
          }
        }
      }
    }
  }

  public static List<Stage> getExecutableStages(List<Stage> stages) {
    List<Stage> executableStages = new ArrayList<>();
    for (Stage stage : stages) {
      StageEntity stageEntity = stage.getStageEntity();

      if (isDependsOnStagesSuccess(stages, stage)) {
        StageExecutionResultType resultType = stageEntity.getResultType();
        if (resultType == null) {
          executableStages.add(stage);
        } else {
          switch (resultType) {
            case ACTIVE:
              executableStages.add(stage);
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
                    && stage.getImmediateExecutionCount() <= immediateRetries) {
                  executableStages.add(stage);
                }
              }
          }
        }
      }
    }

    return executableStages;
  }

  public static boolean isDependsOnStagesSuccess(List<Stage> stages, Stage stage) {
    return getDependsOnStages(stages, stage).stream()
            .filter(s -> s.getStageEntity().getResultType() != StageExecutionResultType.SUCCESS)
            .count()
        == 0;
  }
}
