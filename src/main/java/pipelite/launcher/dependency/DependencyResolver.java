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
import java.util.Collection;
import java.util.List;
import java.util.Set;

import pipelite.entity.StageEntity;
import pipelite.launcher.StageLauncher;
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

  /**
   * Returns the list of stages that the stage depends on directly or transitively.
   *
   * @param stages all stages
   * @param stage the stage of interest
   * @return the list of stages that the stage depends on directly or transitively
   */
  public static List<Stage> getDependsOnStages(List<Stage> stages, Stage stage) {
    List<Stage> dependsOnStages = new ArrayList<>();
    getDependsOnStages(stages, dependsOnStages, stage);
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

  /**
   * Returns the list of stages that may be eventually executed based on the current execution
   * results of all stages in the process
   *
   * @param stages all stages
   * @return the list of stages that may be eventually executed based on the current execution
   *     results of all stages in the process
   */
  public static List<Stage> getEventuallyExecutableStages(List<Stage> stages) {
    List<Stage> executableStages = new ArrayList<>();
    for (Stage stage : stages) {
      if (isEventuallyExecutableStage(stages, stage)) {
        executableStages.add(stage);
      }
    }
    return executableStages;
  }
  /**
   * Returns the list of stages that can be immediately executed.
   *
   * @param stages all stages
   * @param active active stages that are currently being executed
   * @return the list of stages that can be immediately executed
   */
  public static List<Stage> getImmediatelyExecutableStages(
      List<Stage> stages, Collection<Stage> active) {
    List<Stage> executableStages = new ArrayList<>();
    for (Stage stage : stages) {
      if (isImmediatelyExecutableStage(stages, active, stage)) {
        executableStages.add(stage);
      }
    }
    return executableStages;
  }

  /**
   * Returns true if the stage can be eventually executed based on the current execution results of
   * all stages in the process.
   *
   * @param stages all stages
   * @param stage the stage of interest
   * @return true if the stage can be eventually executed based on the current execution results of
   *     all stages in the process
   */
  public static boolean isEventuallyExecutableStage(List<Stage> stages, Stage stage) {
    StageEntity stageEntity = stage.getStageEntity();
    if (stageEntity.getResultType() == StageExecutionResultType.SUCCESS) {
      // Stage can't be executed because it has already been executed successfully.
      return false;
    }
    if (stageEntity.getExecutionCount() > StageLauncher.getMaximumRetries(stage)
        || stage.getImmediateExecutionCount() > StageLauncher.getImmediateRetries(stage)) {
      // Stage can't be executed because it has already been executed the maximum number of times.
      return false;
    }
    for (Stage dependsOn : getDependsOnStages(stages, stage)) {
      if (dependsOn.getStageEntity().getResultType() == StageExecutionResultType.SUCCESS) {
        // Stage can be executed because the stage it depends on has been executed successfully.
        continue;
      }
      if (dependsOn.getStageEntity().getExecutionCount()
              <= StageLauncher.getMaximumRetries(dependsOn)
          || dependsOn.getImmediateExecutionCount()
              <= StageLauncher.getImmediateRetries(dependsOn)) {
        // Stage can be executed because the stage it depends on has not been executed the maximum
        // number of times.
        continue;
      }
      // Stage can't be executed.
      return false;
    }
    // Stage can be executed.
    return true;
  }

  /**
   * Returns true if the stage can be immediately executed.
   *
   * @param stages all stages
   * @param active active stages that are currently being executed
   * @param stage the stage of interest
   * @return true if the stage can be immediately executed
   */
  public static boolean isImmediatelyExecutableStage(
      List<Stage> stages, Collection<Stage> active, Stage stage) {
    if (!isEventuallyExecutableStage(stages, stage)) {
      return false;
    }
    if (active.contains(stage)) {
      // Stage can't be executed because it is already being currently executed.
      return false;
    }
    if (isDependsOnStagesAnyActive(stages, active, stage)) {
      // Stage can't be executed because it depends on stages that are currently being executed.
      return false;
    }
    // Stage can be executed if all the stages it depends on have been successfully executed.
    return isDependsOnStagesAllSuccess(stages, stage);
  }

  /**
   * Returns true if any of the stages the stage depends on are active.
   *
   * @param stages all stages
   * @param active active stages that are currently being executed
   * @param stage the stage of interest
   * @return true if any of the stages the stage depends on are active
   */
  public static boolean isDependsOnStagesAnyActive(
      List<Stage> stages, Collection<Stage> active, Stage stage) {
    for (Stage dependsOn : getDependsOnStages(stages, stage)) {
      if (active.contains(dependsOn)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Returns true if all of the stages the stage depends on have been successfully executed.
   *
   * @param stages all stages
   * @param stage the stage of interest
   * @return true if all of the stages the stage depends on have been successfully executed
   */
  public static boolean isDependsOnStagesAllSuccess(List<Stage> stages, Stage stage) {
    return getDependsOnStages(stages, stage).stream()
            .filter(s -> s.getStageEntity().getResultType() != StageExecutionResultType.SUCCESS)
            .count()
        == 0;
  }
}
