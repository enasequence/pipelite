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

import static org.assertj.core.api.Assertions.assertThat;
import static pipelite.stage.StageExecutionResultType.*;

import java.util.*;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;
import pipelite.UniqueStringGenerator;
import pipelite.entity.StageEntity;
import pipelite.executor.EmptySyncStageExecutor;
import pipelite.executor.StageExecutorParameters;
import pipelite.process.Process;
import pipelite.process.builder.ProcessBuilder;
import pipelite.stage.Stage;
import pipelite.stage.StageExecutionResult;
import pipelite.stage.StageExecutionResultType;

public class DependencyResolverTest {

  @Test
  public void getExecutableStagesAllActiveAllDependOnPrevious() {
    List<Stage> stages = new ArrayList<>();

    ProcessBuilder builder = new ProcessBuilder(UniqueStringGenerator.randomProcessId());
    Process process =
        builder
            .execute("STAGE1")
            .withEmptySyncExecutor(StageExecutionResultType.SUCCESS)
            .executeAfterPrevious("STAGE2")
            .withEmptySyncExecutor(StageExecutionResultType.SUCCESS)
            .executeAfterPrevious("STAGE3")
            .withEmptySyncExecutor(StageExecutionResultType.SUCCESS)
            .build();

    for (Stage stage : process.getStages()) {
      StageEntity stageEntity = new StageEntity();
      stage.setStageEntity(stageEntity);
      stageEntity.startExecution(stage);
      stages.add(stage);
    }

    List<Stage> immediatelyExecutableStages =
        DependencyResolver.getImmediatelyExecutableStages(stages, Collections.emptySet());
    assertThat(immediatelyExecutableStages.size()).isOne();
    assertThat(immediatelyExecutableStages.get(0).getStageName()).isEqualTo("STAGE1");

    List<Stage> eventuallyExecutableStages =
        DependencyResolver.getEventuallyExecutableStages(stages);
    assertThat(eventuallyExecutableStages.size()).isEqualTo(3);
    assertThat(eventuallyExecutableStages.get(0).getStageName()).isEqualTo("STAGE1");
    assertThat(eventuallyExecutableStages.get(1).getStageName()).isEqualTo("STAGE2");
    assertThat(eventuallyExecutableStages.get(2).getStageName()).isEqualTo("STAGE3");
  }

  @Test
  public void getExecutableStagesAllActiveAllDependOnFirst() {
    List<Stage> stages = new ArrayList<>();

    ProcessBuilder builder = new ProcessBuilder(UniqueStringGenerator.randomProcessId());
    Process process =
        builder
            .execute("STAGE1")
            .withEmptySyncExecutor(StageExecutionResultType.SUCCESS)
            .executeAfter("STAGE2", "STAGE1")
            .withEmptySyncExecutor(StageExecutionResultType.SUCCESS)
            .executeAfter("STAGE3", "STAGE1")
            .withEmptySyncExecutor(StageExecutionResultType.SUCCESS)
            .build();

    for (Stage stage : process.getStages()) {
      StageEntity stageEntity = new StageEntity();
      stage.setStageEntity(stageEntity);
      stageEntity.startExecution(stage);
      stages.add(stage);
    }

    List<Stage> immediatelyExecutableStages =
        DependencyResolver.getImmediatelyExecutableStages(stages, Collections.emptySet());
    assertThat(immediatelyExecutableStages.size()).isOne();
    assertThat(immediatelyExecutableStages.get(0).getStageName()).isEqualTo("STAGE1");

    List<Stage> eventuallyExecutableStages =
        DependencyResolver.getEventuallyExecutableStages(stages);
    assertThat(eventuallyExecutableStages.size()).isEqualTo(3);
    assertThat(eventuallyExecutableStages.get(0).getStageName()).isEqualTo("STAGE1");
    assertThat(eventuallyExecutableStages.get(1).getStageName()).isEqualTo("STAGE2");
    assertThat(eventuallyExecutableStages.get(2).getStageName()).isEqualTo("STAGE3");
  }

  @Test
  public void getExecutableStagesFirstSuccessAllDependOnFirst() {
    List<Stage> stages = new ArrayList<>();

    ProcessBuilder builder = new ProcessBuilder(UniqueStringGenerator.randomProcessId());
    Process process =
        builder
            .execute("STAGE1")
            .withEmptySyncExecutor(StageExecutionResultType.SUCCESS)
            .executeAfter("STAGE2", "STAGE1")
            .withEmptySyncExecutor(StageExecutionResultType.SUCCESS)
            .executeAfter("STAGE3", "STAGE1")
            .withEmptySyncExecutor(StageExecutionResultType.SUCCESS)
            .build();

    int stageNumber = 0;
    for (Stage stage : process.getStages()) {
      StageEntity stageEntity = new StageEntity();
      stage.setStageEntity(stageEntity);
      if (stageNumber == 0) {
        stageEntity.startExecution(stage);
        stageEntity.endExecution(StageExecutionResult.success());
      }
      stageNumber++;
      stages.add(stage);
    }

    List<Stage> immediatelyExecutableStages =
        DependencyResolver.getImmediatelyExecutableStages(stages, Collections.emptySet());
    assertThat(immediatelyExecutableStages.size()).isEqualTo(2);
    assertThat(immediatelyExecutableStages.get(0).getStageName()).isEqualTo("STAGE2");
    assertThat(immediatelyExecutableStages.get(1).getStageName()).isEqualTo("STAGE3");

    List<Stage> eventuallyExecutableStages =
        DependencyResolver.getEventuallyExecutableStages(stages);
    assertThat(eventuallyExecutableStages.size()).isEqualTo(2);
    assertThat(eventuallyExecutableStages.get(0).getStageName()).isEqualTo("STAGE2");
    assertThat(eventuallyExecutableStages.get(1).getStageName()).isEqualTo("STAGE3");
  }

  @Test
  public void getExecutableStagesFirstErrorMaxRetriesAllDependOnFirst() {
    List<Stage> stages = new ArrayList<>();

    ProcessBuilder builder = new ProcessBuilder(UniqueStringGenerator.randomProcessId());
    Process process =
        builder
            .execute("STAGE1", StageExecutorParameters.builder().maximumRetries(0).build())
            .withEmptySyncExecutor(StageExecutionResultType.SUCCESS)
            .executeAfter("STAGE2", "STAGE1")
            .withEmptySyncExecutor(StageExecutionResultType.SUCCESS)
            .executeAfter("STAGE3", "STAGE1")
            .withEmptySyncExecutor(StageExecutionResultType.SUCCESS)
            .build();

    int stageNumber = 0;
    for (Stage stage : process.getStages()) {
      StageEntity stageEntity = new StageEntity();
      stage.setStageEntity(stageEntity);
      if (stageNumber == 0) {
        stageEntity.startExecution(stage);
        stageEntity.endExecution(StageExecutionResult.error());
        stage.incrementImmediateExecutionCount();
        stages.add(stage);
      }
      stageNumber++;
    }

    List<Stage> immediatelyExecutableStages =
        DependencyResolver.getImmediatelyExecutableStages(stages, Collections.emptySet());
    assertThat(immediatelyExecutableStages.size()).isEqualTo(0);

    List<Stage> eventuallyExecutableStages =
        DependencyResolver.getEventuallyExecutableStages(stages);
    assertThat(eventuallyExecutableStages.size()).isEqualTo(0);
  }

  @Test
  public void getExecutableStagesFirstErrorMaxImmediateAllDependOnFirst() {
    List<Stage> stages = new ArrayList<>();

    ProcessBuilder builder = new ProcessBuilder(UniqueStringGenerator.randomProcessId());
    Process process =
        builder
            .execute(
                "STAGE1",
                StageExecutorParameters.builder().maximumRetries(3).immediateRetries(0).build())
            .withEmptySyncExecutor(StageExecutionResultType.SUCCESS)
            .executeAfter("STAGE2", "STAGE1")
            .withEmptySyncExecutor(StageExecutionResultType.SUCCESS)
            .executeAfter("STAGE3", "STAGE1")
            .withEmptySyncExecutor(StageExecutionResultType.SUCCESS)
            .build();

    int stageNumber = 0;
    for (Stage stage : process.getStages()) {
      StageEntity stageEntity = new StageEntity();
      stage.setStageEntity(stageEntity);
      if (stageNumber == 0) {
        stageEntity.startExecution(stage);
        stageEntity.endExecution(StageExecutionResult.error());
        stage.incrementImmediateExecutionCount();
        stages.add(stage);
      }
      stageNumber++;
    }

    List<Stage> immediatelyExecutableStages =
        DependencyResolver.getImmediatelyExecutableStages(stages, Collections.emptySet());
    assertThat(immediatelyExecutableStages.size()).isEqualTo(0);

    List<Stage> eventuallyExecutableStages =
        DependencyResolver.getEventuallyExecutableStages(stages);
    assertThat(eventuallyExecutableStages.size()).isEqualTo(0);
  }

  @Test
  public void getExecutableStagesFirstErrorNotMaxRetriesImmediateAllDependOnFirst() {
    List<Stage> stages = new ArrayList<>();

    ProcessBuilder builder = new ProcessBuilder(UniqueStringGenerator.randomProcessId());
    Process process =
        builder
            .execute(
                "STAGE1",
                StageExecutorParameters.builder().maximumRetries(1).immediateRetries(1).build())
            .withEmptySyncExecutor(StageExecutionResultType.SUCCESS)
            .executeAfter("STAGE2", "STAGE1")
            .withEmptySyncExecutor(StageExecutionResultType.SUCCESS)
            .executeAfter("STAGE3", "STAGE1")
            .withEmptySyncExecutor(StageExecutionResultType.SUCCESS)
            .build();

    int stageNumber = 0;
    for (Stage stage : process.getStages()) {
      StageEntity stageEntity = new StageEntity();
      stage.setStageEntity(stageEntity);
      if (stageNumber == 0) {
        stageEntity.startExecution(stage);
        stageEntity.endExecution(StageExecutionResult.error());
        stage.incrementImmediateExecutionCount();
        stages.add(stage);
      }
      stageNumber++;
    }

    List<Stage> immediatelyExecutableStages =
        DependencyResolver.getImmediatelyExecutableStages(stages, Collections.emptySet());
    assertThat(immediatelyExecutableStages.size()).isOne();
    assertThat(immediatelyExecutableStages.get(0).getStageName()).isEqualTo("STAGE1");

    List<Stage> eventuallyExecutableStages =
        DependencyResolver.getEventuallyExecutableStages(stages);
    assertThat(eventuallyExecutableStages.size()).isOne();
    assertThat(eventuallyExecutableStages.get(0).getStageName()).isEqualTo("STAGE1");
  }

  @Test
  public void getDependentStagesAllDependOnPrevious() {
    List<Stage> stages = new ArrayList<>();

    ProcessBuilder builder = new ProcessBuilder(UniqueStringGenerator.randomProcessId());
    Process process =
        builder
            .execute("STAGE1")
            .withEmptySyncExecutor(StageExecutionResultType.SUCCESS)
            .executeAfterPrevious("STAGE2")
            .withEmptySyncExecutor(StageExecutionResultType.SUCCESS)
            .executeAfterPrevious("STAGE3")
            .withEmptySyncExecutor(StageExecutionResultType.SUCCESS)
            .executeAfterPrevious("STAGE4")
            .withEmptySyncExecutor(StageExecutionResultType.SUCCESS)
            .build();

    for (Stage stage : process.getStages()) {
      StageEntity stageEntity = new StageEntity();
      stage.setStageEntity(stageEntity);
      stages.add(stage);
    }

    List<Stage> dependentStages = DependencyResolver.getDependentStages(stages, stages.get(0));
    assertThat(dependentStages.size()).isEqualTo(3);
    assertThat(dependentStages.get(0).getStageName()).isEqualTo("STAGE4");
    assertThat(dependentStages.get(1).getStageName()).isEqualTo("STAGE3");
    assertThat(dependentStages.get(2).getStageName()).isEqualTo("STAGE2");

    dependentStages = DependencyResolver.getDependentStages(stages, stages.get(1));
    assertThat(dependentStages.size()).isEqualTo(2);
    assertThat(dependentStages.get(0).getStageName()).isEqualTo("STAGE4");
    assertThat(dependentStages.get(1).getStageName()).isEqualTo("STAGE3");

    dependentStages = DependencyResolver.getDependentStages(stages, stages.get(2));
    assertThat(dependentStages.size()).isEqualTo(1);
    assertThat(dependentStages.get(0).getStageName()).isEqualTo("STAGE4");
  }

  @Test
  public void getDependentStagesAllDependOnFirst() {
    List<Stage> stages = new ArrayList<>();

    ProcessBuilder builder = new ProcessBuilder(UniqueStringGenerator.randomProcessId());
    Process process =
        builder
            .execute("STAGE1")
            .withEmptySyncExecutor(StageExecutionResultType.SUCCESS)
            .executeAfter("STAGE2", "STAGE1")
            .withEmptySyncExecutor(StageExecutionResultType.SUCCESS)
            .executeAfter("STAGE3", "STAGE1")
            .withEmptySyncExecutor(StageExecutionResultType.SUCCESS)
            .executeAfter("STAGE4", "STAGE1")
            .withEmptySyncExecutor(StageExecutionResultType.SUCCESS)
            .build();

    for (Stage stage : process.getStages()) {
      StageEntity stageEntity = new StageEntity();
      stage.setStageEntity(stageEntity);
      stages.add(stage);
    }

    List<Stage> dependentStages = DependencyResolver.getDependentStages(stages, stages.get(0));
    assertThat(dependentStages.size()).isEqualTo(3);
    assertThat(dependentStages.get(0).getStageName()).isEqualTo("STAGE2");
    assertThat(dependentStages.get(1).getStageName()).isEqualTo("STAGE3");
    assertThat(dependentStages.get(2).getStageName()).isEqualTo("STAGE4");

    dependentStages = DependencyResolver.getDependentStages(stages, stages.get(1));
    assertThat(dependentStages.size()).isEqualTo(0);

    dependentStages = DependencyResolver.getDependentStages(stages, stages.get(2));
    assertThat(dependentStages.size()).isEqualTo(0);

    dependentStages = DependencyResolver.getDependentStages(stages, stages.get(3));
    assertThat(dependentStages.size()).isEqualTo(0);
  }

  private Stage getStageByStageName(List<Stage> stages, String stageName) {
    for (Stage stage : stages) {
      if (stage.getStageName().equals(stageName)) {
        return stage;
      }
    }
    return null;
  }

  /**
   * Tests getDependsOnStages. Given a list of stages tests that a stage depends on other stages.
   *
   * @param stages the list of stages
   * @param stageName the stage name of interest
   * @param expectedDependsOnStageNames the list of stage names the stage should depend on
   */
  private void getDependsOnStages(
      List<Stage> stages, String stageName, List<String> expectedDependsOnStageNames) {
    List<Stage> dependsOnStages =
        DependencyResolver.getDependsOnStages(stages, getStageByStageName(stages, stageName));
    List<String> dependsOnStageNames =
        dependsOnStages.stream().map(s -> s.getStageName()).collect(Collectors.toList());
    assertThat(expectedDependsOnStageNames)
        .containsExactlyInAnyOrderElementsOf(dependsOnStageNames);
  }

  /**
   * Tests getDependsOnStages. Given a list of stages tests that a stage depends on other stages.
   */
  @Test
  public void getDependsOnStages() {
    ProcessBuilder builder = new ProcessBuilder(UniqueStringGenerator.randomProcessId());
    Process process =
        builder
            .execute("STAGE1")
            .withEmptySyncExecutor(StageExecutionResultType.SUCCESS)
            .executeAfter("STAGE2", "STAGE1")
            .withEmptySyncExecutor(StageExecutionResultType.SUCCESS)
            .executeAfter("STAGE3", Arrays.asList("STAGE1", "STAGE2"))
            .withEmptySyncExecutor(StageExecutionResultType.SUCCESS)
            .executeAfter("STAGE4", Arrays.asList("STAGE3", "STAGE3"))
            .withEmptySyncExecutor(StageExecutionResultType.SUCCESS)
            .build();

    List<Stage> stages = new ArrayList<>();
    for (Stage stage : process.getStages()) {
      StageEntity stageEntity = new StageEntity();
      stage.setStageEntity(stageEntity);
      stages.add(stage);
    }

    getDependsOnStages(stages, "STAGE1", Arrays.asList());
    getDependsOnStages(stages, "STAGE2", Arrays.asList("STAGE1"));
    getDependsOnStages(stages, "STAGE3", Arrays.asList("STAGE1", "STAGE2"));
    getDependsOnStages(stages, "STAGE4", Arrays.asList("STAGE1", "STAGE2", "STAGE3"));
  }

  /**
   * Creates a process with three stages. Two independent stages, STAGE0 and STAGE1, and a third
   * stage STAGE2 that depends on both STAGE0 and STAGE1.
   *
   * @param firstStageResultType the stage execution result type for STAGE0
   * @param secondStageResultType the stage execution result type for STAGE1
   * @return a list of all three stages
   */
  public static List<Stage> isDependsOnStageSuccessStages(
      StageExecutionResultType firstStageResultType,
      StageExecutionResultType secondStageResultType) {
    Process process =
        new ProcessBuilder("test")
            .execute("STAGE0")
            .with((pipelineName, processId, stage) -> null)
            .execute("STAGE1")
            .with((pipelineName, processId, stage) -> null)
            .executeAfter("STAGE2", Arrays.asList("STAGE0", "STAGE1"))
            .with((pipelineName, processId, stage) -> null)
            .build();
    List<Stage> stages = new ArrayList<>();
    Stage firstStage = process.getStages().get(0);
    StageEntity firstStageEntity = new StageEntity();
    firstStage.setStageEntity(firstStageEntity);
    firstStageEntity.setResultType(firstStageResultType);
    stages.add(firstStage);

    Stage secondStage = process.getStages().get(1);
    StageEntity secondStageEntity = new StageEntity();
    secondStage.setStageEntity(secondStageEntity);
    secondStageEntity.setResultType(secondStageResultType);
    stages.add(secondStage);

    Stage lastStage = process.getStages().get(2);
    StageEntity lastStageEntity = new StageEntity();
    lastStage.setStageEntity(lastStageEntity);
    stages.add(lastStage);

    return stages;
  }

  /**
   * Tests isDependsOnStagesAllSuccess using a process with three stages. Two independent stages,
   * STAGE0 and STAGE1, and a third stage STAGE2 that depends on both STAGE0 and STAGE1.
   *
   * @param firstStageResultType the stage execution result for STAGE0
   * @param secondStageResultType the stage execution result for STAGE1
   * @param expectedReturnValue the expected return value of isDependsOnStagesAllSuccess for STAGE2
   */
  private void isDependsOnAllStagesSuccess(
      StageExecutionResultType firstStageResultType,
      StageExecutionResultType secondStageResultType,
      boolean expectedReturnValue) {
    List<Stage> stages = isDependsOnStageSuccessStages(firstStageResultType, secondStageResultType);
    assertThat(DependencyResolver.isDependsOnStagesAllSuccess(stages, stages.get(2)))
        .isEqualTo(expectedReturnValue);
  }

  /**
   * Tests isDependsOnStagesAllSuccess using a process with three stages. Two independent stages,
   * STAGE0 and STAGE1, and a third stage STAGE2 that depends on both STAGE0 and STAGE1.
   */
  @Test
  public void isDependsOnAllStagesSuccess() {
    isDependsOnAllStagesSuccess(null, null, false);
    isDependsOnAllStagesSuccess(null, ACTIVE, false);
    isDependsOnAllStagesSuccess(null, SUCCESS, false);
    isDependsOnAllStagesSuccess(null, ERROR, false);
    isDependsOnAllStagesSuccess(ACTIVE, null, false);
    isDependsOnAllStagesSuccess(ACTIVE, ACTIVE, false);
    isDependsOnAllStagesSuccess(ACTIVE, SUCCESS, false);
    isDependsOnAllStagesSuccess(ACTIVE, ERROR, false);
    isDependsOnAllStagesSuccess(ERROR, null, false);
    isDependsOnAllStagesSuccess(ERROR, ACTIVE, false);
    isDependsOnAllStagesSuccess(ERROR, SUCCESS, false);
    isDependsOnAllStagesSuccess(ERROR, ERROR, false);
    isDependsOnAllStagesSuccess(SUCCESS, null, false);
    isDependsOnAllStagesSuccess(SUCCESS, ACTIVE, false);
    isDependsOnAllStagesSuccess(SUCCESS, SUCCESS, true);
    isDependsOnAllStagesSuccess(SUCCESS, ERROR, false);
  }

  /**
   * Tests isImmediatelyExecutableStage using a process with one stage.
   *
   * @param resultType the stage execution result for the stage
   * @param executionCount the stage execution count for the stage
   * @param immediateCount the immediate stage execution count for the stage
   * @param maximumRetries the number of maximum retries for the stage
   * @param immediateRetries the number of immediate retries for the stage
   */
  private boolean isImmediatelyExecutableSingleStage(
      StageExecutionResultType resultType,
      int executionCount,
      int immediateCount,
      int maximumRetries,
      int immediateRetries) {
    StageEntity stageEntity = new StageEntity();
    stageEntity.setExecutionCount(executionCount);
    stageEntity.setResultType(resultType);
    Stage stage =
        Stage.builder()
            .stageName("STAGE")
            .executor(new EmptySyncStageExecutor(StageExecutionResultType.SUCCESS))
            .executorParams(
                StageExecutorParameters.builder()
                    .immediateRetries(immediateRetries)
                    .maximumRetries(maximumRetries)
                    .build())
            .build();
    stage.setStageEntity(stageEntity);
    for (int i = 0; i < immediateCount; ++i) {
      stage.incrementImmediateExecutionCount();
    }
    return DependencyResolver.isImmediatelyExecutableStage(
        Arrays.asList(stage), Collections.emptySet(), stage);
  }

  /** Tests isImmediatelyExecutableStage using a process with one stage. */
  @Test
  public void isImmediatelyExecutableSingleStage() {
    for (int executionCount = 0; executionCount < 2; executionCount++) {
      for (int immediateCount = 0; immediateCount < 2; immediateCount++) {
        for (int maximumRetries = 0; maximumRetries < 2; maximumRetries++) {
          for (int immediateRetries = 0; immediateRetries < 2; immediateRetries++) {
            assertThat(
                    isImmediatelyExecutableSingleStage(
                        SUCCESS, executionCount, immediateCount, maximumRetries, immediateRetries))
                .isFalse();
            if (executionCount <= maximumRetries
                && immediateCount <= Math.min(immediateRetries, maximumRetries)) {
              System.out.println(executionCount);
              System.out.println(maximumRetries);
              System.out.println(immediateCount);
              System.out.println(immediateRetries);
              assertThat(
                      isImmediatelyExecutableSingleStage(
                          null, executionCount, immediateCount, maximumRetries, immediateRetries))
                  .isTrue();
              assertThat(
                      isImmediatelyExecutableSingleStage(
                          ERROR, executionCount, immediateCount, maximumRetries, immediateRetries))
                  .isTrue();
              assertThat(
                      isImmediatelyExecutableSingleStage(
                          ACTIVE, executionCount, immediateCount, maximumRetries, immediateRetries))
                  .isTrue();
            }
          }
        }
      }
    }
  }

  /**
   * Tests isEventuallyExecutableStage using a process with one stage.
   *
   * @param resultType the stage execution result for the stage
   * @param executionCount the stage execution count for the stage
   * @param immediateCount the immediate stage execution count for the stage
   * @param maximumRetries the number of maximum retries for the stage
   * @param immediateRetries the number of immediate retries for the stage
   */
  private boolean isEventuallyExecutableSingleStage(
      StageExecutionResultType resultType,
      int executionCount,
      int immediateCount,
      int maximumRetries,
      int immediateRetries) {
    StageEntity stageEntity = new StageEntity();
    stageEntity.setExecutionCount(executionCount);
    stageEntity.setResultType(resultType);
    Stage stage =
        Stage.builder()
            .stageName("STAGE")
            .executor(new EmptySyncStageExecutor(StageExecutionResultType.SUCCESS))
            .executorParams(
                StageExecutorParameters.builder()
                    .immediateRetries(immediateRetries)
                    .maximumRetries(maximumRetries)
                    .build())
            .build();
    stage.setStageEntity(stageEntity);
    for (int i = 0; i < immediateCount; ++i) {
      stage.incrementImmediateExecutionCount();
    }
    return DependencyResolver.isEventuallyExecutableStage(Arrays.asList(stage), stage);
  }

  /** Tests isEventuallyExecutableStage using a process with one stage. */
  @Test
  public void isEventuallyExecutableSingleStage() {
    for (int executionCount = 0; executionCount < 2; executionCount++) {
      for (int immediateCount = 0; immediateCount < 2; immediateCount++) {
        for (int maximumRetries = 0; maximumRetries < 2; maximumRetries++) {
          for (int immediateRetries = 0; immediateRetries < 2; immediateRetries++) {
            assertThat(
                    isEventuallyExecutableSingleStage(
                        SUCCESS, executionCount, immediateCount, maximumRetries, immediateRetries))
                .isFalse();
            if (executionCount <= maximumRetries
                && immediateCount <= Math.min(immediateRetries, maximumRetries)) {
              System.out.println(executionCount);
              System.out.println(maximumRetries);
              System.out.println(immediateCount);
              System.out.println(immediateRetries);
              assertThat(
                      isEventuallyExecutableSingleStage(
                          null, executionCount, immediateCount, maximumRetries, immediateRetries))
                  .isTrue();
              assertThat(
                      isEventuallyExecutableSingleStage(
                          ERROR, executionCount, immediateCount, maximumRetries, immediateRetries))
                  .isTrue();
              assertThat(
                      isEventuallyExecutableSingleStage(
                          ACTIVE, executionCount, immediateCount, maximumRetries, immediateRetries))
                  .isTrue();
            }
          }
        }
      }
    }
  }
}
