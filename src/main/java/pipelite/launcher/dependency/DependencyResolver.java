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

  private DependencyResolver() {};

  public static List<ProcessLauncher.StageExecution> getDependentStages(
      List<ProcessLauncher.StageExecution> stages, ProcessLauncher.StageExecution from) {
    List<ProcessLauncher.StageExecution> dependentStages = new ArrayList<>();
    getDependentStages(stages, dependentStages, from, false);
    return dependentStages;
  }

  private static void getDependentStages(
      List<ProcessLauncher.StageExecution> stages,
      List<ProcessLauncher.StageExecution> dependentStages,
      ProcessLauncher.StageExecution from,
      boolean include) {

    for (ProcessLauncher.StageExecution stageExecution : stages) {
      Stage stage = stageExecution.getStage();
      if (stage.equals(from)) {
        continue;
      }

      if (stage.getDependsOn() != null) {
        for (Stage dependsOn : stage.getDependsOn()) {
          if (dependsOn.getStageName().equals(from.getStage().getStageName())) {
            getDependentStages(stages, dependentStages, stageExecution, true);
          }
        }
      }
    }

    if (include) {
      dependentStages.add(from);
    }
  }

  public static List<ProcessLauncher.StageExecution> getDependsOnStages(
      List<ProcessLauncher.StageExecution> stages, ProcessLauncher.StageExecution from) {
    List<ProcessLauncher.StageExecution> dependsOnStages = new ArrayList<>();
    getDependsOnStages(stages, dependsOnStages, from);
    return dependsOnStages;
  }

  private static void getDependsOnStages(
      List<ProcessLauncher.StageExecution> stages,
      List<ProcessLauncher.StageExecution> dependsOnStages,
      ProcessLauncher.StageExecution from) {

    if (from.getStage().getDependsOn() != null) {
      for (Stage dependsOn : from.getStage().getDependsOn()) {
        for (ProcessLauncher.StageExecution stageExecution : stages) {
          Stage stage = stageExecution.getStage();
          if (stage.equals(dependsOn)) {
            if (!dependsOnStages.contains(stageExecution)) {
              dependsOnStages.add(stageExecution);
              getDependsOnStages(stages, dependsOnStages, stageExecution);
            }
          }
        }
      }
    }
  }

  public static List<ProcessLauncher.StageExecution> getExecutableStages(
      List<ProcessLauncher.StageExecution> stages) {
    List<ProcessLauncher.StageExecution> executableStages = new ArrayList<>();
    for (ProcessLauncher.StageExecution stageExecution : stages) {
      Stage stage = stageExecution.getStage();
      StageEntity stageEntity = stageExecution.getStageEntity();

      if (isDependsOnStagesSuccess(stages, stageExecution)) {
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

  public static boolean isDependsOnStagesSuccess(
      List<ProcessLauncher.StageExecution> stages, ProcessLauncher.StageExecution stage) {
    return getDependsOnStages(stages, stage).stream()
            .filter(s -> s.getStageEntity().getResultType() != StageExecutionResultType.SUCCESS)
            .count()
        == 0;
  }
}
