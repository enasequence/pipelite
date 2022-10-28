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
package pipelite.runner.stage;

import java.util.*;
import java.util.stream.Collectors;
import pipelite.process.Process;
import pipelite.stage.Stage;

public class DependencyResolver {

  private DependencyResolver() {}

  /**
   * Returns the list of stages that depend on this stage directly or transitively.
   *
   * @param process the process
   * @param stage the stage of interest
   * @return the list of stages that depend on this stage directly or transitively
   */
  public static List<Stage> getDependentStages(Process process, Stage stage) {
    Set<Stage> dependentStages = new HashSet<>();
    getDependentStages(process, stage, false, dependentStages);
    return new ArrayList<>(dependentStages);
  }

  private static void getDependentStages(
      Process process, Stage stage, boolean include, Set<Stage> dependentStages) {
    process
        .getStageGraph()
        .edgesOf(stage)
        .forEach(
            e -> {
              Stage edgeSource = process.getStageGraph().getEdgeSource(e);
              Stage edgeTarget = process.getStageGraph().getEdgeTarget(e);
              if (edgeSource.equals(stage)) {
                dependentStages.add(edgeTarget);
                getDependentStages(process, edgeTarget, true, dependentStages);
              }
            });
    if (include) {
      dependentStages.add(stage);
    }
  }

  /**
   * Returns the list of stages that the stage depends on directly or transitively.
   *
   * @param process the process
   * @param stage the stage of interest
   * @return the list of stages that the stage depends on directly or transitively
   */
  public static Set<Stage> getDependsOnStages(Process process, Stage stage) {
    Set<Stage> dependsOnStages = new HashSet<>();
    getDependsOnStages(process, stage, dependsOnStages);
    return dependsOnStages;
  }

  private static void getDependsOnStages(Process process, Stage stage, Set<Stage> dependsOnStages) {
    process
        .getStageGraph()
        .edgesOf(stage)
        .forEach(
            e -> {
              Stage edgeSource = process.getStageGraph().getEdgeSource(e);
              Stage edgeTarget = process.getStageGraph().getEdgeTarget(e);
              if (edgeTarget.equals(stage)) {
                dependsOnStages.add(edgeSource);
                getDependsOnStages(process, edgeSource, dependsOnStages);
              }
            });
  }

  /**
   * Returns the list of stages that the stage depends on directly.
   *
   * @param process the process
   * @param stage the stage of interest
   * @return the list of stages that the stage depends on directly
   */
  public static Set<Stage> getDependsOnStagesDirectly(Process process, Stage stage) {
    Set<Stage> dependsOnStages = new HashSet<>();
    process
        .getStageGraph()
        .edgesOf(stage)
        .forEach(
            e -> {
              Stage edgeSource = process.getStageGraph().getEdgeSource(e);
              Stage edgeTarget = process.getStageGraph().getEdgeTarget(e);
              if (edgeTarget.equals(stage)) {
                dependsOnStages.add(edgeSource);
              }
            });
    return dependsOnStages;
  }

  /**
   * Returns the list of stages that may be eventually executed based on the current execution
   * results of all stages in the process
   *
   * @param process the process
   * @return the list of stages that may be eventually executed based on the current execution
   *     results of all stages in the process
   */
  public static List<Stage> getEventuallyExecutableStages(Process process) {
    return process.getStages().stream()
        .filter(stage -> isEventuallyExecutableStage(process, stage))
        .collect(Collectors.toList());
  }

  /**
   * Returns the list of stages that have failed and exceeded the maximum execution count
   *
   * @param stages all stages
   * @return the list of stages that have failed and exceeded the maximum execution count
   */
  public static List<Stage> getPermanentlyFailedStages(Collection<Stage> stages) {
    return stages.stream()
        .filter(stage -> isPermanentlyFailedStage(stage))
        .collect(Collectors.toList());
  }

  /**
   * Returns the list of stages that can be immediately executed.
   *
   * @param process the process
   * @param active active stages that are currently being executed
   * @return the list of stages that can be immediately executed
   */
  public static List<Stage> getImmediatelyExecutableStages(
      Process process, Collection<Stage> active) {
    return process.getStages().stream()
        .filter(stage -> isImmediatelyExecutableStage(process, active, stage))
        .collect(Collectors.toList());
  }

  /**
   * Returns true if the stage can be eventually executed based on the current execution results of
   * all stages in the process.
   *
   * @param process the process
   * @param stage the stage of interest
   * @return true if the stage can be eventually executed based on the current execution results of
   *     all stages in the process
   */
  public static boolean isEventuallyExecutableStage(Process process, Stage stage) {
    if (stage.isSuccess()) {
      // Stage can't be executed as it has already been executed successfully.
      return false;
    }
    if (stage.isError()) {
      if (stage.isPermanentError()) {
        // Permanent error prevents further executions.
        return false;
      }

      // Stage that has previously failed execution can be executed if it has not been retried the
      // maximum number of times.
      return stage.hasMaximumRetriesLeft();
    }
    // Stage is pending or active.
    for (Stage dependsOn : getDependsOnStages(process, stage)) {
      if (isPermanentlyFailedStage(dependsOn)) {
        // Stage can't be executed if it depends on a permanently failed stage.
        return false;
      }
    }
    // Stage can be executed.
    return true;
  }

  /**
   * Returns true if the stage can be immediately executed.
   *
   * @param process the process
   * @param active active stages that are currently being executed
   * @param stage the stage of interest
   * @return true if the stage can be immediately executed
   */
  public static boolean isImmediatelyExecutableStage(
      Process process, Collection<Stage> active, Stage stage) {
    if (active.contains(stage)) {
      // Stage can't be executed as it is currently being executed.
      return false;
    }

    Set<Stage> dependsOnStages = getDependsOnStages(process, stage);

    if (dependsOnStages.stream().anyMatch(s -> active.contains(s))) {
      // Stage can't be executed as it depends on stages that are currently being executed.
      return false;
    }

    if (!isEventuallyExecutableStage(process, stage)) {
      // Stage can't be executed as it is not eventually executable.
      return false;
    }

    if (stage.isError()) {
      if (stage.isPermanentError()) {
        // Permanent error prevents further executions.
        return false;
      }

      // Stage that has previously failed execution can be executed if it has not been retried the
      // maximum number of times.
      return stage.hasImmediateRetriesLeft();
    }

    // Stage can be executed if all stages it depends on have been executed successfully.
    return dependsOnStages.stream().allMatch(s -> s.isSuccess());
  }

  /**
   * Returns true if the stage has failed and exceeded the maximum execution count or if the error
   * is a permanent error
   *
   * @param stage the stage of interest
   * @return true if the stage has failed and exceeded the maximum execution count or if the error
   *     is a permanent error
   */
  public static boolean isPermanentlyFailedStage(Stage stage) {
    return (stage.isError() && !stage.hasMaximumRetriesLeft()) || stage.isPermanentError();
  }
}
