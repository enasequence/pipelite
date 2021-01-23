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

import java.util.*;
import java.util.stream.Collectors;

import pipelite.stage.Stage;

public class DependencyResolver {

  private DependencyResolver() {}

  /**
   * Returns the list of stages that depend on this stage directly or transitively.
   *
   * @param stages all stages
   * @param stage the stage of interest
   * @return the list of stages that depend on this stage directly or transitively
   */
  public static List<Stage> getDependentStages(List<Stage> stages, Stage stage) {
    Set<Stage> dependentStages = new HashSet<>();
    getDependentStages(stages, dependentStages, stage, false);
    return new ArrayList<>(dependentStages);
  }

  private static void getDependentStages(
      List<Stage> stages, Set<Stage> dependentStages, Stage from, boolean include) {

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
    return stages.stream()
        .filter(stage -> isEventuallyExecutableStage(stages, stage))
        .collect(Collectors.toList());
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
    return stages.stream()
        .filter(stage -> isImmediatelyExecutableStage(stages, active, stage))
        .collect(Collectors.toList());
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
    if (stage.isSuccess()) {
      // Stage can't be executed as it has already been executed successfully.
      return false;
    }
    if (stage.isError()) {
      // Stage that has previously failed execution can be executed if it has not been retried the
      // maximum number of times.
      return stage.hasMaximumRetriesLeft();
    }
    // Stage is pending or active.
    for (Stage dependsOn : getDependsOnStages(stages, stage)) {
      if (dependsOn.isError() && !dependsOn.hasMaximumRetriesLeft()) {
        // Stage can't be executed if it depends on a stage that has been retried the maximum number
        // of times.
        return false;
      }
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
    if (active.contains(stage)) {
      // Stage can't be executed as it is currently being executed.
      return false;
    }

    List<Stage> dependsOnStages = getDependsOnStages(stages, stage);

    if (dependsOnStages.stream().anyMatch(s -> active.contains(s))) {
      // Stage can't be executed as it depends on stages that are currently being executed.
      return false;
    }

    if (!isEventuallyExecutableStage(stages, stage)) {
      // Stage can't be executed as it is not eventually executable.
      return false;
    }

    if (stage.isError()) {
      // Stage that has previously failed execution can be executed if it has not been retried the
      // maximum number of times.
      return stage.hasImmediateRetriesLeft();
    }

    // Stage can be executed if all stages it depends on have been executed successfully.
    return dependsOnStages.stream().allMatch(s -> s.isSuccess());
  }
}
