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
package pipelite.runner.stage;

import static org.assertj.core.api.Assertions.assertThat;
import static pipelite.runner.stage.DependencyResolver.isEventuallyExecutableStage;
import static pipelite.runner.stage.DependencyResolver.isImmediatelyExecutableStage;
import static pipelite.stage.StageState.*;

import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import pipelite.UniqueStringGenerator;
import pipelite.entity.StageEntity;
import pipelite.executor.TestExecutor;
import pipelite.process.Process;
import pipelite.process.builder.ProcessBuilder;
import pipelite.stage.Stage;
import pipelite.stage.StageState;
import pipelite.stage.executor.StageExecutorResult;
import pipelite.stage.parameters.ExecutorParameters;

public class DependencyResolverTest {

  @Test
  public void allActivePendingDependOnPrevious() {
    for (StageState stageState : EnumSet.of(ACTIVE, PENDING)) {

      ProcessBuilder builder =
          new ProcessBuilder(UniqueStringGenerator.randomProcessId(DependencyResolverTest.class));
      Process process =
          builder
              .execute("STAGE1")
              .withSyncTestExecutor()
              .executeAfterPrevious("STAGE2")
              .withSyncTestExecutor()
              .executeAfterPrevious("STAGE3")
              .withSyncTestExecutor()
              .build();
      List<Stage> stages = process.getStages();
      for (Stage stage : process.getStages()) {
        createStageEntity(stage, stageState);
      }

      assertGetDependsOnStages(stages, "STAGE1", Arrays.asList());
      assertGetDependsOnStages(stages, "STAGE2", Arrays.asList("STAGE1"));
      assertGetDependsOnStages(stages, "STAGE3", Arrays.asList("STAGE1", "STAGE2"));

      assertGetDependentStages(stages, "STAGE1", Arrays.asList("STAGE2", "STAGE3"));
      assertGetDependentStages(stages, "STAGE2", Arrays.asList("STAGE3"));
      assertGetDependentStages(stages, "STAGE3", Arrays.asList());

      assertGetPermanentFailedStages(stages, Arrays.asList());
      assertGetImmediatelyExecutableStages(stages, Arrays.asList("STAGE1"));
      assertGetEventuallyExecutableStages(stages, Arrays.asList("STAGE1", "STAGE2", "STAGE3"));
    }
  }

  @Test
  public void allActivePendingOthersDependOnFirstAllActive() {
    for (StageState stageState : EnumSet.of(ACTIVE, PENDING)) {
      ProcessBuilder builder =
          new ProcessBuilder(UniqueStringGenerator.randomProcessId(DependencyResolverTest.class));
      Process process =
          builder
              .execute("STAGE1")
              .withSyncTestExecutor()
              .executeAfter("STAGE2", "STAGE1")
              .withSyncTestExecutor()
              .executeAfter("STAGE3", "STAGE1")
              .withSyncTestExecutor()
              .build();
      List<Stage> stages = process.getStages();
      for (Stage stage : process.getStages()) {
        createStageEntity(stage, stageState);
      }

      assertGetDependsOnStages(stages, "STAGE1", Arrays.asList());
      assertGetDependsOnStages(stages, "STAGE2", Arrays.asList("STAGE1"));
      assertGetDependsOnStages(stages, "STAGE3", Arrays.asList("STAGE1"));

      assertGetDependentStages(stages, "STAGE1", Arrays.asList("STAGE2", "STAGE3"));
      assertGetDependentStages(stages, "STAGE2", Arrays.asList());
      assertGetDependentStages(stages, "STAGE3", Arrays.asList());

      assertGetPermanentFailedStages(stages, Arrays.asList());
      assertGetImmediatelyExecutableStages(stages, Arrays.asList("STAGE1"));
      assertGetEventuallyExecutableStages(stages, Arrays.asList("STAGE1", "STAGE2", "STAGE3"));
    }
  }

  @Test
  public void othersActivePendingDependOnFirstThatSucceeded() {
    for (StageState stageState : EnumSet.of(ACTIVE, PENDING)) {

      ProcessBuilder builder =
          new ProcessBuilder(UniqueStringGenerator.randomProcessId(DependencyResolverTest.class));
      Process process =
          builder
              .execute("STAGE1")
              .withSyncTestExecutor()
              .executeAfter("STAGE2", "STAGE1")
              .withSyncTestExecutor()
              .executeAfter("STAGE3", "STAGE1")
              .withSyncTestExecutor()
              .build();
      List<Stage> stages = process.getStages();
      int stageNumber = 0;
      for (Stage stage : process.getStages()) {
        if (stageNumber == 0) {
          createStageEntity(stage, StageState.SUCCESS);
        } else {
          createStageEntity(stage, stageState);
        }
        stageNumber++;
      }

      assertGetDependsOnStages(stages, "STAGE1", Arrays.asList());
      assertGetDependsOnStages(stages, "STAGE2", Arrays.asList("STAGE1"));
      assertGetDependsOnStages(stages, "STAGE3", Arrays.asList("STAGE1"));

      assertGetDependentStages(stages, "STAGE1", Arrays.asList("STAGE2", "STAGE3"));
      assertGetDependentStages(stages, "STAGE2", Arrays.asList());
      assertGetDependentStages(stages, "STAGE3", Arrays.asList());

      assertGetPermanentFailedStages(stages, Arrays.asList());
      assertGetImmediatelyExecutableStages(stages, Arrays.asList("STAGE2", "STAGE3"));
      assertGetEventuallyExecutableStages(stages, Arrays.asList("STAGE2", "STAGE3"));
    }
  }

  @Test
  public void othersActivePendingDependOnFirstInErrorNoRetriesLeft() {
    for (StageState stageState : EnumSet.of(ACTIVE, PENDING)) {

      ProcessBuilder builder =
          new ProcessBuilder(UniqueStringGenerator.randomProcessId(DependencyResolverTest.class));
      Process process =
          builder
              .execute("STAGE1")
              .withSyncTestExecutor(ERROR, ExecutorParameters.builder().maximumRetries(0).build())
              .executeAfter("STAGE2", "STAGE1")
              .withSyncTestExecutor()
              .executeAfter("STAGE3", "STAGE1")
              .withSyncTestExecutor()
              .build();
      List<Stage> stages = process.getStages();
      int stageNumber = 0;
      for (Stage stage : process.getStages()) {
        if (stageNumber == 0) {
          createStageEntity(stage, StageState.ERROR);
        } else {
          createStageEntity(stage, stageState);
        }
        stageNumber++;
      }

      assertGetDependsOnStages(stages, "STAGE1", Arrays.asList());
      assertGetDependsOnStages(stages, "STAGE2", Arrays.asList("STAGE1"));
      assertGetDependsOnStages(stages, "STAGE3", Arrays.asList("STAGE1"));

      assertGetDependentStages(stages, "STAGE1", Arrays.asList("STAGE2", "STAGE3"));
      assertGetDependentStages(stages, "STAGE2", Arrays.asList());
      assertGetDependentStages(stages, "STAGE3", Arrays.asList());

      assertGetPermanentFailedStages(stages, Arrays.asList("STAGE1"));
      assertGetImmediatelyExecutableStages(stages, Arrays.asList());
      assertGetEventuallyExecutableStages(stages, Arrays.asList());
    }
  }

  @Test
  public void othersActivePendingDependOnFirstInErrorMaxRetriesLeft() {
    for (StageState stageState : EnumSet.of(ACTIVE, PENDING)) {

      ProcessBuilder builder =
          new ProcessBuilder(UniqueStringGenerator.randomProcessId(DependencyResolverTest.class));
      Process process =
          builder
              .execute("STAGE1")
              .withSyncTestExecutor(
                  ERROR, ExecutorParameters.builder().maximumRetries(3).immediateRetries(0).build())
              .executeAfter("STAGE2", "STAGE1")
              .withSyncTestExecutor()
              .executeAfter("STAGE3", "STAGE1")
              .withSyncTestExecutor()
              .build();
      List<Stage> stages = process.getStages();
      int stageNumber = 0;
      for (Stage stage : process.getStages()) {
        if (stageNumber == 0) {
          createStageEntity(stage, StageState.ERROR);
        } else {
          createStageEntity(stage, stageState);
        }
        stageNumber++;
      }

      assertGetDependsOnStages(stages, "STAGE1", Arrays.asList());
      assertGetDependsOnStages(stages, "STAGE2", Arrays.asList("STAGE1"));
      assertGetDependsOnStages(stages, "STAGE3", Arrays.asList("STAGE1"));

      assertGetDependentStages(stages, "STAGE1", Arrays.asList("STAGE2", "STAGE3"));
      assertGetDependentStages(stages, "STAGE2", Arrays.asList());
      assertGetDependentStages(stages, "STAGE3", Arrays.asList());

      assertGetPermanentFailedStages(stages, Arrays.asList());
      assertGetImmediatelyExecutableStages(stages, Arrays.asList());
      assertGetEventuallyExecutableStages(stages, Arrays.asList("STAGE1", "STAGE2", "STAGE3"));
    }
  }

  @Test
  public void othersActivePendingIndependentOneErrorMaxAndImmediateRetriesLeft() {
    for (StageState stageState : EnumSet.of(ACTIVE, PENDING)) {
      ProcessBuilder builder =
          new ProcessBuilder(UniqueStringGenerator.randomProcessId(DependencyResolverTest.class));
      Process process =
          builder
              .execute("STAGE1")
              .withSyncTestExecutor(
                  ERROR, ExecutorParameters.builder().maximumRetries(1).immediateRetries(1).build())
              .execute("STAGE2")
              .withSyncTestExecutor()
              .execute("STAGE3")
              .withSyncTestExecutor()
              .build();
      List<Stage> stages = process.getStages();
      int stageNumber = 0;
      for (Stage stage : process.getStages()) {
        if (stageNumber == 0) {
          createStageEntity(stage, StageState.ERROR);
        } else {
          createStageEntity(stage, stageState);
        }
        stageNumber++;
      }

      assertGetDependsOnStages(stages, "STAGE1", Arrays.asList());
      assertGetDependsOnStages(stages, "STAGE2", Arrays.asList());
      assertGetDependsOnStages(stages, "STAGE3", Arrays.asList());

      assertGetDependentStages(stages, "STAGE1", Arrays.asList());
      assertGetDependentStages(stages, "STAGE2", Arrays.asList());
      assertGetDependentStages(stages, "STAGE3", Arrays.asList());

      assertGetPermanentFailedStages(stages, Arrays.asList());
      assertGetImmediatelyExecutableStages(stages, Arrays.asList("STAGE1", "STAGE2", "STAGE3"));
      assertGetEventuallyExecutableStages(stages, Arrays.asList("STAGE1", "STAGE2", "STAGE3"));
    }
  }

  @Test
  public void othersActivePendingIndependentOneErrorMaxRetriesLeft() {
    for (StageState stageState : EnumSet.of(ACTIVE, PENDING)) {
      ProcessBuilder builder =
          new ProcessBuilder(UniqueStringGenerator.randomProcessId(DependencyResolverTest.class));
      Process process =
          builder
              .execute("STAGE1")
              .withSyncTestExecutor(
                  ERROR, ExecutorParameters.builder().maximumRetries(1).immediateRetries(0).build())
              .execute("STAGE2")
              .withSyncTestExecutor()
              .execute("STAGE3")
              .withSyncTestExecutor()
              .execute("STAGE4")
              .withSyncTestExecutor()
              .build();
      List<Stage> stages = process.getStages();
      int stageNumber = 0;
      for (Stage stage : process.getStages()) {
        if (stageNumber == 0) {
          createStageEntity(stage, StageState.ERROR);
        } else {
          createStageEntity(stage, stageState);
        }
        stageNumber++;
      }

      assertGetDependsOnStages(stages, "STAGE1", Arrays.asList());
      assertGetDependsOnStages(stages, "STAGE2", Arrays.asList());
      assertGetDependsOnStages(stages, "STAGE3", Arrays.asList());
      assertGetDependsOnStages(stages, "STAGE4", Arrays.asList());

      assertGetDependentStages(stages, "STAGE1", Arrays.asList());
      assertGetDependentStages(stages, "STAGE2", Arrays.asList());
      assertGetDependentStages(stages, "STAGE3", Arrays.asList());
      assertGetDependentStages(stages, "STAGE4", Arrays.asList());

      assertGetPermanentFailedStages(stages, Arrays.asList());
      assertGetImmediatelyExecutableStages(stages, Arrays.asList("STAGE2", "STAGE3", "STAGE4"));
      assertGetEventuallyExecutableStages(
          stages, Arrays.asList("STAGE1", "STAGE2", "STAGE3", "STAGE4"));
    }
  }

  @Test
  public void othersActivePendingIndependentOneErrorNoRetriesLeft() {
    for (StageState stageState : EnumSet.of(ACTIVE, PENDING)) {
      ProcessBuilder builder =
          new ProcessBuilder(UniqueStringGenerator.randomProcessId(DependencyResolverTest.class));
      Process process =
          builder
              .execute("STAGE1")
              .withSyncTestExecutor(
                  ERROR, ExecutorParameters.builder().maximumRetries(0).immediateRetries(0).build())
              .execute("STAGE2")
              .withSyncTestExecutor()
              .execute("STAGE3")
              .withSyncTestExecutor()
              .execute("STAGE4")
              .withSyncTestExecutor()
              .build();
      List<Stage> stages = process.getStages();
      int stageNumber = 0;
      for (Stage stage : stages) {
        if (stageNumber == 0) {
          createStageEntity(stage, StageState.ERROR);
        } else {
          createStageEntity(stage, stageState);
        }
        stageNumber++;
      }
      assertGetDependsOnStages(stages, "STAGE1", Arrays.asList());
      assertGetDependsOnStages(stages, "STAGE2", Arrays.asList());
      assertGetDependsOnStages(stages, "STAGE3", Arrays.asList());
      assertGetDependsOnStages(stages, "STAGE4", Arrays.asList());

      assertGetDependentStages(stages, "STAGE1", Arrays.asList());
      assertGetDependentStages(stages, "STAGE2", Arrays.asList());
      assertGetDependentStages(stages, "STAGE3", Arrays.asList());
      assertGetDependentStages(stages, "STAGE4", Arrays.asList());

      assertGetPermanentFailedStages(stages, Arrays.asList("STAGE1"));
      assertGetImmediatelyExecutableStages(stages, Arrays.asList("STAGE2", "STAGE3", "STAGE4"));
      assertGetEventuallyExecutableStages(stages, Arrays.asList("STAGE2", "STAGE3", "STAGE4"));
    }
  }

  @Test
  public void allActivePendingOthersDependOnPrevious() {
    for (StageState stageState : EnumSet.of(ACTIVE, PENDING)) {
      ProcessBuilder builder =
          new ProcessBuilder(UniqueStringGenerator.randomProcessId(DependencyResolverTest.class));
      Process process =
          builder
              .execute("STAGE1")
              .withSyncTestExecutor()
              .executeAfterPrevious("STAGE2")
              .withSyncTestExecutor()
              .executeAfterPrevious("STAGE3")
              .withSyncTestExecutor()
              .executeAfterPrevious("STAGE4")
              .withSyncTestExecutor()
              .build();
      List<Stage> stages = process.getStages();
      for (Stage stage : stages) {
        stage.setStageEntity(new StageEntity());
        stage.getStageEntity().setStageState(stageState);
      }
      assertGetDependsOnStages(stages, "STAGE1", Arrays.asList());
      assertGetDependsOnStages(stages, "STAGE2", Arrays.asList("STAGE1"));
      assertGetDependsOnStages(stages, "STAGE3", Arrays.asList("STAGE1", "STAGE2"));
      assertGetDependsOnStages(stages, "STAGE4", Arrays.asList("STAGE1", "STAGE2", "STAGE3"));

      assertGetDependentStages(stages, "STAGE1", Arrays.asList("STAGE2", "STAGE3", "STAGE4"));
      assertGetDependentStages(stages, "STAGE2", Arrays.asList("STAGE3", "STAGE4"));
      assertGetDependentStages(stages, "STAGE3", Arrays.asList("STAGE4"));
      assertGetDependentStages(stages, "STAGE4", Arrays.asList());

      assertGetPermanentFailedStages(stages, Arrays.asList());
      assertGetImmediatelyExecutableStages(stages, Arrays.asList("STAGE1"));
      assertGetEventuallyExecutableStages(
          stages, Arrays.asList("STAGE1", "STAGE2", "STAGE3", "STAGE4"));
    }
  }

  @Test
  public void allActivePendingOthersDependOnFirstTransitively() {
    for (StageState stageState : EnumSet.of(ACTIVE, PENDING)) {
      ProcessBuilder builder =
          new ProcessBuilder(UniqueStringGenerator.randomProcessId(DependencyResolverTest.class));
      Process process =
          builder
              .execute("STAGE1")
              .withSyncTestExecutor()
              .executeAfter("STAGE2", "STAGE1")
              .withSyncTestExecutor()
              .executeAfter("STAGE3", Arrays.asList("STAGE2"))
              .withSyncTestExecutor()
              .executeAfter("STAGE4", Arrays.asList("STAGE3", "STAGE3"))
              .withSyncTestExecutor()
              .build();
      List<Stage> stages = process.getStages();
      for (Stage stage : stages) {
        stage.setStageEntity(new StageEntity());
        stage.getStageEntity().setStageState(stageState);
      }
      assertGetDependsOnStages(stages, "STAGE1", Arrays.asList());
      assertGetDependsOnStages(stages, "STAGE2", Arrays.asList("STAGE1"));
      assertGetDependsOnStages(stages, "STAGE3", Arrays.asList("STAGE1", "STAGE2"));
      assertGetDependsOnStages(stages, "STAGE4", Arrays.asList("STAGE1", "STAGE2", "STAGE3"));

      assertGetDependentStages(stages, "STAGE1", Arrays.asList("STAGE2", "STAGE3", "STAGE4"));
      assertGetDependentStages(stages, "STAGE2", Arrays.asList("STAGE3", "STAGE4"));
      assertGetDependentStages(stages, "STAGE3", Arrays.asList("STAGE4"));
      assertGetDependentStages(stages, "STAGE4", Arrays.asList());

      assertGetPermanentFailedStages(stages, Arrays.asList());
      assertGetImmediatelyExecutableStages(stages, Arrays.asList("STAGE1"));
      assertGetEventuallyExecutableStages(
          stages, Arrays.asList("STAGE1", "STAGE2", "STAGE3", "STAGE4"));
    }
  }

  @Test
  public void independentAllErrorNoRetriesLeft() {
    ProcessBuilder builder =
        new ProcessBuilder(UniqueStringGenerator.randomProcessId(DependencyResolverTest.class));
    Process process =
        builder
            .execute("STAGE1")
            .withSyncTestExecutor(
                ERROR, ExecutorParameters.builder().maximumRetries(0).immediateRetries(0).build())
            .execute("STAGE2")
            .withSyncTestExecutor(
                ERROR, ExecutorParameters.builder().maximumRetries(0).immediateRetries(0).build())
            .execute("STAGE3")
            .withSyncTestExecutor(
                ERROR, ExecutorParameters.builder().maximumRetries(0).immediateRetries(0).build())
            .execute("STAGE4")
            .withSyncTestExecutor(
                ERROR, ExecutorParameters.builder().maximumRetries(0).immediateRetries(0).build())
            .build();
    List<Stage> stages = process.getStages();
    for (Stage stage : stages) {
      createStageEntity(stage, StageState.ERROR);
    }
    assertGetDependsOnStages(stages, "STAGE1", Arrays.asList());
    assertGetDependsOnStages(stages, "STAGE2", Arrays.asList());
    assertGetDependsOnStages(stages, "STAGE3", Arrays.asList());
    assertGetDependsOnStages(stages, "STAGE4", Arrays.asList());

    assertGetDependentStages(stages, "STAGE1", Arrays.asList());
    assertGetDependentStages(stages, "STAGE2", Arrays.asList());
    assertGetDependentStages(stages, "STAGE3", Arrays.asList());
    assertGetDependentStages(stages, "STAGE4", Arrays.asList());

    assertGetPermanentFailedStages(stages, Arrays.asList("STAGE1", "STAGE2", "STAGE3", "STAGE4"));
    assertGetImmediatelyExecutableStages(stages, Arrays.asList());
    assertGetEventuallyExecutableStages(stages, Arrays.asList());
  }

  @Test
  public void isImmediatelyExecutableSingleStage() {
    for (int totalCnt = 0; totalCnt < 2; totalCnt++) {
      for (int immedCnt = 0; immedCnt < 2; immedCnt++) {
        for (int maxRetry = 0; maxRetry < 2; maxRetry++) {
          for (int immedRetry = 0; immedRetry < 2; immedRetry++) {
            List<Stage> pending = getStage(PENDING, totalCnt, immedCnt, maxRetry, immedRetry);
            List<Stage> active = getStage(ACTIVE, totalCnt, immedCnt, maxRetry, immedRetry);
            List<Stage> error = getStage(ERROR, totalCnt, immedCnt, maxRetry, immedRetry);
            List<Stage> success = getStage(SUCCESS, totalCnt, immedCnt, maxRetry, immedRetry);
            assertIsImmediatelyExecutableStage(pending, active.get(0), true);
            assertIsImmediatelyExecutableStage(active, active.get(0), true);
            assertIsImmediatelyExecutableStage(
                error,
                error.get(0),
                totalCnt <= maxRetry && immedCnt <= Math.min(immedRetry, maxRetry));
            assertIsImmediatelyExecutableStage(success, success.get(0), false);
          }
        }
      }
    }
  }

  @Test
  public void isEventuallyExecutableSingleStage() {
    for (int totalCnt = 0; totalCnt < 2; totalCnt++) {
      for (int immedCnt = 0; immedCnt < 2; immedCnt++) {
        for (int maxRetry = 0; maxRetry < 2; maxRetry++) {
          for (int immedRetry = 0; immedRetry < 2; immedRetry++) {
            List<Stage> pending = getStage(PENDING, totalCnt, immedCnt, maxRetry, immedRetry);
            List<Stage> active = getStage(ACTIVE, totalCnt, immedCnt, maxRetry, immedRetry);
            List<Stage> error = getStage(ERROR, totalCnt, immedCnt, maxRetry, immedRetry);
            List<Stage> success = getStage(SUCCESS, totalCnt, immedCnt, maxRetry, immedRetry);
            assertIsEventuallyExecutableStage(pending, active.get(0), true);
            assertIsEventuallyExecutableStage(active, active.get(0), true);
            assertIsEventuallyExecutableStage(error, error.get(0), totalCnt <= maxRetry);
            assertIsEventuallyExecutableStage(success, success.get(0), false);
          }
        }
      }
    }
  }

  /** Returns a single stage with the given stage state, execution counts and retries. */
  private List<Stage> getStage(
      StageState stageState,
      int executionCount,
      int immediateCount,
      int maximumRetries,
      int immediateRetries) {
    StageEntity stageEntity = new StageEntity();
    stageEntity.setExecutionCount(executionCount);
    stageEntity.setStageState(stageState);

    TestExecutor executor = TestExecutor.sync(StageState.SUCCESS);
    executor.setExecutorParams(
        ExecutorParameters.builder()
            .immediateRetries(immediateRetries)
            .maximumRetries(maximumRetries)
            .build());
    Stage stage = Stage.builder().stageName("STAGE").executor(executor).build();
    stage.setStageEntity(stageEntity);
    for (int i = 0; i < immediateCount; ++i) {
      stage.incrementImmediateExecutionCount();
    }
    return Arrays.asList(stage);
  }

  private void assertIsImmediatelyExecutableStage(
      List<Stage> stages, Stage stage, boolean expected) {
    assertThat(isImmediatelyExecutableStage(stages, Collections.emptySet(), stage))
        .isEqualTo(expected);
  }

  private void assertIsEventuallyExecutableStage(
      List<Stage> stages, Stage stage, boolean expected) {
    assertThat(isEventuallyExecutableStage(stages, stage)).isEqualTo(expected);
  }

  private void assertGetDependentStages(
      List<Stage> stages, String stageName, List<String> expectedDependentStageNames) {
    List<Stage> dependentStages =
        DependencyResolver.getDependentStages(stages, getStageByStageName(stages, stageName));
    List<String> dependentStageNames =
        dependentStages.stream().map(s -> s.getStageName()).collect(Collectors.toList());
    assertThat(expectedDependentStageNames)
        .containsExactlyInAnyOrderElementsOf(dependentStageNames);
  }

  private void assertGetDependsOnStages(
      List<Stage> stages, String stageName, List<String> expectedDependsOnStageNames) {
    List<Stage> dependsOnStages =
        DependencyResolver.getDependsOnStages(stages, getStageByStageName(stages, stageName));
    List<String> dependsOnStageNames =
        dependsOnStages.stream().map(s -> s.getStageName()).collect(Collectors.toList());
    assertThat(expectedDependsOnStageNames)
        .containsExactlyInAnyOrderElementsOf(dependsOnStageNames);
  }

  private void assertGetPermanentFailedStages(
      List<Stage> stages, List<String> expectedPermanentlyFailedStageNames) {
    List<Stage> permanentlyFailedStages = DependencyResolver.getPermanentlyFailedStages(stages);
    List<String> permanentlyFailedStageNames =
        permanentlyFailedStages.stream().map(s -> s.getStageName()).collect(Collectors.toList());
    assertThat(expectedPermanentlyFailedStageNames)
        .containsExactlyInAnyOrderElementsOf(permanentlyFailedStageNames);
  }

  private void assertGetImmediatelyExecutableStages(
      List<Stage> stages, List<String> expectedImmediatelyExecutableStageNames) {
    List<Stage> immediatelyExecutableStages =
        DependencyResolver.getImmediatelyExecutableStages(stages, Collections.emptySet());
    List<String> immediatelyExecutableStageNames =
        immediatelyExecutableStages.stream()
            .map(s -> s.getStageName())
            .collect(Collectors.toList());
    assertThat(expectedImmediatelyExecutableStageNames)
        .containsExactlyInAnyOrderElementsOf(immediatelyExecutableStageNames);
  }

  private void assertGetEventuallyExecutableStages(
      List<Stage> stages, List<String> expectedEventuallyExecutableStageNames) {
    List<Stage> eventuallyExecutableStages =
        DependencyResolver.getEventuallyExecutableStages(stages);
    List<String> eventuallyExecutableStageNames =
        eventuallyExecutableStages.stream().map(s -> s.getStageName()).collect(Collectors.toList());
    assertThat(expectedEventuallyExecutableStageNames)
        .containsExactlyInAnyOrderElementsOf(eventuallyExecutableStageNames);
  }

  private Stage getStageByStageName(List<Stage> stages, String stageName) {
    for (Stage stage : stages) {
      if (stage.getStageName().equals(stageName)) {
        return stage;
      }
    }
    return null;
  }

  private static void createStageEntity(Stage stage, StageState stageState) {
    StageEntity stageEntity = new StageEntity();
    stageEntity.setStageState(stageState);
    stage.setStageEntity(stageEntity);
    if (stageState == PENDING) {
      return;
    }
    stageEntity.startExecution(stage);
    if (stageState == ACTIVE) {
      return;
    }
    stage.incrementImmediateExecutionCount();
    if (stageState == SUCCESS) {
      stageEntity.endExecution(StageExecutorResult.success());
      return;
    }
    if (stageState == ERROR) {
      stageEntity.endExecution(StageExecutorResult.error());
      return;
    }
  }
}
