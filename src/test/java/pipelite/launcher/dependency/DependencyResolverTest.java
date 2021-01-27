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
import static pipelite.launcher.dependency.DependencyResolver.isEventuallyExecutableStage;
import static pipelite.launcher.dependency.DependencyResolver.isImmediatelyExecutableStage;
import static pipelite.stage.StageState.*;

import java.util.*;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import pipelite.UniqueStringGenerator;
import pipelite.entity.StageEntity;
import pipelite.executor.CallExecutor;
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
              .withCallExecutor()
              .executeAfterPrevious("STAGE2")
              .withCallExecutor()
              .executeAfterPrevious("STAGE3")
              .withCallExecutor()
              .build();
      List<Stage> stages = process.getStages();
      for (Stage stage : process.getStages()) {
        StageEntity stageEntity = new StageEntity();
        stageEntity.setStageState(stageState);
        stage.setStageEntity(stageEntity);
        stageEntity.startExecution(stage);
      }

      assertGetDependsOnStagesIsTrue(stages, "STAGE1", Arrays.asList());
      assertGetDependsOnStagesIsTrue(stages, "STAGE2", Arrays.asList("STAGE1"));
      assertGetDependsOnStagesIsTrue(stages, "STAGE3", Arrays.asList("STAGE1", "STAGE2"));

      assertGetDependentStagesIsTrue(stages, "STAGE1", Arrays.asList("STAGE2", "STAGE3"));
      assertGetDependentStagesIsTrue(stages, "STAGE2", Arrays.asList("STAGE3"));
      assertGetDependentStagesIsTrue(stages, "STAGE3", Arrays.asList());

      assertIsImmediatelyExecutableStage(stages, process, "STAGE1", true);
      assertIsImmediatelyExecutableStage(stages, process, "STAGE2", false);
      assertIsImmediatelyExecutableStage(stages, process, "STAGE3", false);

      assertIsEventuallyExecutableStage(stages, process, "STAGE1", true);
      assertIsEventuallyExecutableStage(stages, process, "STAGE2", true);
      assertIsEventuallyExecutableStage(stages, process, "STAGE3", true);
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
              .withCallExecutor()
              .executeAfter("STAGE2", "STAGE1")
              .withCallExecutor()
              .executeAfter("STAGE3", "STAGE1")
              .withCallExecutor()
              .build();
      List<Stage> stages = process.getStages();
      for (Stage stage : process.getStages()) {
        StageEntity stageEntity = new StageEntity();
        stageEntity.setStageState(stageState);
        stage.setStageEntity(stageEntity);
        stageEntity.startExecution(stage);
      }

      assertGetDependsOnStagesIsTrue(stages, "STAGE1", Arrays.asList());
      assertGetDependsOnStagesIsTrue(stages, "STAGE2", Arrays.asList("STAGE1"));
      assertGetDependsOnStagesIsTrue(stages, "STAGE3", Arrays.asList("STAGE1"));

      assertGetDependentStagesIsTrue(stages, "STAGE1", Arrays.asList("STAGE2", "STAGE3"));
      assertGetDependentStagesIsTrue(stages, "STAGE2", Arrays.asList());
      assertGetDependentStagesIsTrue(stages, "STAGE3", Arrays.asList());

      assertIsImmediatelyExecutableStage(stages, process, "STAGE1", true);
      assertIsImmediatelyExecutableStage(stages, process, "STAGE2", false);
      assertIsImmediatelyExecutableStage(stages, process, "STAGE3", false);

      assertIsEventuallyExecutableStage(stages, process, "STAGE1", true);
      assertIsEventuallyExecutableStage(stages, process, "STAGE2", true);
      assertIsEventuallyExecutableStage(stages, process, "STAGE3", true);
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
              .withCallExecutor()
              .executeAfter("STAGE2", "STAGE1")
              .withCallExecutor()
              .executeAfter("STAGE3", "STAGE1")
              .withCallExecutor()
              .build();
      List<Stage> stages = process.getStages();
      int stageNumber = 0;
      for (Stage stage : process.getStages()) {
        StageEntity stageEntity = new StageEntity();
        stage.setStageEntity(stageEntity);
        if (stageNumber == 0) {
          stageEntity.startExecution(stage);
          stageEntity.endExecution(StageExecutorResult.success());
        } else {
          stageEntity.setStageState(stageState);
        }
        stageNumber++;
      }

      assertGetDependsOnStagesIsTrue(stages, "STAGE1", Arrays.asList());
      assertGetDependsOnStagesIsTrue(stages, "STAGE2", Arrays.asList("STAGE1"));
      assertGetDependsOnStagesIsTrue(stages, "STAGE3", Arrays.asList("STAGE1"));

      assertGetDependentStagesIsTrue(stages, "STAGE1", Arrays.asList("STAGE2", "STAGE3"));
      assertGetDependentStagesIsTrue(stages, "STAGE2", Arrays.asList());
      assertGetDependentStagesIsTrue(stages, "STAGE3", Arrays.asList());

      assertIsImmediatelyExecutableStage(stages, process, "STAGE1", false);
      assertIsImmediatelyExecutableStage(stages, process, "STAGE2", true);
      assertIsImmediatelyExecutableStage(stages, process, "STAGE3", true);

      assertIsEventuallyExecutableStage(stages, process, "STAGE1", false);
      assertIsEventuallyExecutableStage(stages, process, "STAGE2", true);
      assertIsEventuallyExecutableStage(stages, process, "STAGE3", true);
    }
  }

  @Test
  public void othersActivePendingDependOnFirstThatFailedMaximum() {
    for (StageState stageState : EnumSet.of(ACTIVE, PENDING)) {

      ProcessBuilder builder =
          new ProcessBuilder(UniqueStringGenerator.randomProcessId(DependencyResolverTest.class));
      Process process =
          builder
              .execute("STAGE1")
              .withCallExecutor(ERROR, ExecutorParameters.builder().maximumRetries(0).build())
              .executeAfter("STAGE2", "STAGE1")
              .withCallExecutor()
              .executeAfter("STAGE3", "STAGE1")
              .withCallExecutor()
              .build();
      List<Stage> stages = process.getStages();
      int stageNumber = 0;
      for (Stage stage : process.getStages()) {
        StageEntity stageEntity = new StageEntity();
        stage.setStageEntity(stageEntity);
        if (stageNumber == 0) {
          stageEntity.startExecution(stage);
          stageEntity.endExecution(StageExecutorResult.error());
          stage.incrementImmediateExecutionCount();
        } else {
          stageEntity.setStageState(stageState);
        }
        stageNumber++;
      }

      assertGetDependsOnStagesIsTrue(stages, "STAGE1", Arrays.asList());
      assertGetDependsOnStagesIsTrue(stages, "STAGE2", Arrays.asList("STAGE1"));
      assertGetDependsOnStagesIsTrue(stages, "STAGE3", Arrays.asList("STAGE1"));

      assertGetDependentStagesIsTrue(stages, "STAGE1", Arrays.asList("STAGE2", "STAGE3"));
      assertGetDependentStagesIsTrue(stages, "STAGE2", Arrays.asList());
      assertGetDependentStagesIsTrue(stages, "STAGE3", Arrays.asList());

      assertIsImmediatelyExecutableStage(stages, process, "STAGE1", false);
      assertIsImmediatelyExecutableStage(stages, process, "STAGE2", false);
      assertIsImmediatelyExecutableStage(stages, process, "STAGE3", false);

      assertIsEventuallyExecutableStage(stages, process, "STAGE1", false);
      assertIsEventuallyExecutableStage(stages, process, "STAGE2", false);
      assertIsEventuallyExecutableStage(stages, process, "STAGE3", false);
    }
  }

  @Test
  public void othersActivePendingDependOnFirstThatFailedImmediate() {
    for (StageState stageState : EnumSet.of(ACTIVE, PENDING)) {

      ProcessBuilder builder =
          new ProcessBuilder(UniqueStringGenerator.randomProcessId(DependencyResolverTest.class));
      Process process =
          builder
              .execute("STAGE1")
              .withCallExecutor(
                  ERROR, ExecutorParameters.builder().maximumRetries(3).immediateRetries(0).build())
              .executeAfter("STAGE2", "STAGE1")
              .withCallExecutor()
              .executeAfter("STAGE3", "STAGE1")
              .withCallExecutor()
              .build();
      List<Stage> stages = process.getStages();
      int stageNumber = 0;
      for (Stage stage : process.getStages()) {
        StageEntity stageEntity = new StageEntity();
        stage.setStageEntity(stageEntity);
        if (stageNumber == 0) {
          stageEntity.startExecution(stage);
          stageEntity.endExecution(StageExecutorResult.error());
          stage.incrementImmediateExecutionCount();
        } else {
          stageEntity.setStageState(stageState);
        }
        stageNumber++;
      }

      assertGetDependsOnStagesIsTrue(stages, "STAGE1", Arrays.asList());
      assertGetDependsOnStagesIsTrue(stages, "STAGE2", Arrays.asList("STAGE1"));
      assertGetDependsOnStagesIsTrue(stages, "STAGE3", Arrays.asList("STAGE1"));

      assertGetDependentStagesIsTrue(stages, "STAGE1", Arrays.asList("STAGE2", "STAGE3"));
      assertGetDependentStagesIsTrue(stages, "STAGE2", Arrays.asList());
      assertGetDependentStagesIsTrue(stages, "STAGE3", Arrays.asList());

      assertIsImmediatelyExecutableStage(stages, process, "STAGE1", false);
      assertIsImmediatelyExecutableStage(stages, process, "STAGE2", false);
      assertIsImmediatelyExecutableStage(stages, process, "STAGE3", false);

      assertIsEventuallyExecutableStage(stages, process, "STAGE1", true);
      assertIsEventuallyExecutableStage(stages, process, "STAGE2", true);
      assertIsEventuallyExecutableStage(stages, process, "STAGE3", true);
    }
  }

  @Test
  public void othersActivePendingIndependentOneError() {
    for (StageState stageState : EnumSet.of(ACTIVE, PENDING)) {
      ProcessBuilder builder =
          new ProcessBuilder(UniqueStringGenerator.randomProcessId(DependencyResolverTest.class));
      Process process =
          builder
              .execute("STAGE1")
              .withCallExecutor(
                  ERROR, ExecutorParameters.builder().maximumRetries(1).immediateRetries(1).build())
              .execute("STAGE2")
              .withCallExecutor()
              .execute("STAGE3")
              .withCallExecutor()
              .build();
      List<Stage> stages = process.getStages();
      int stageNumber = 0;
      for (Stage stage : process.getStages()) {
        StageEntity stageEntity = new StageEntity();
        stage.setStageEntity(stageEntity);
        if (stageNumber == 0) {
          stageEntity.startExecution(stage);
          stageEntity.endExecution(StageExecutorResult.error());
          stage.incrementImmediateExecutionCount();
        } else {
          stageEntity.setStageState(stageState);
        }
        stageNumber++;
      }

      assertGetDependsOnStagesIsTrue(stages, "STAGE1", Arrays.asList());
      assertGetDependsOnStagesIsTrue(stages, "STAGE2", Arrays.asList());
      assertGetDependsOnStagesIsTrue(stages, "STAGE3", Arrays.asList());

      assertGetDependentStagesIsTrue(stages, "STAGE1", Arrays.asList());
      assertGetDependentStagesIsTrue(stages, "STAGE2", Arrays.asList());
      assertGetDependentStagesIsTrue(stages, "STAGE3", Arrays.asList());

      assertIsImmediatelyExecutableStage(stages, process, "STAGE1", true);
      assertIsImmediatelyExecutableStage(stages, process, "STAGE2", true);
      assertIsImmediatelyExecutableStage(stages, process, "STAGE3", true);

      assertIsEventuallyExecutableStage(stages, process, "STAGE1", true);
      assertIsEventuallyExecutableStage(stages, process, "STAGE2", true);
      assertIsEventuallyExecutableStage(stages, process, "STAGE3", true);
    }
  }

  @Test
  public void othersActivePendingIndependentOneFailedImmediate() {
    for (StageState stageState : EnumSet.of(ACTIVE, PENDING)) {
      ProcessBuilder builder =
          new ProcessBuilder(UniqueStringGenerator.randomProcessId(DependencyResolverTest.class));
      Process process =
          builder
              .execute("STAGE1")
              .withCallExecutor(
                  ERROR, ExecutorParameters.builder().maximumRetries(1).immediateRetries(0).build())
              .execute("STAGE2")
              .withCallExecutor()
              .execute("STAGE3")
              .withCallExecutor()
              .execute("STAGE4")
              .withCallExecutor()
              .build();
      List<Stage> stages = process.getStages();
      int stageNumber = 0;
      for (Stage stage : process.getStages()) {
        StageEntity stageEntity = new StageEntity();
        stage.setStageEntity(stageEntity);
        if (stageNumber == 0) {
          stageEntity.startExecution(stage);
          stageEntity.endExecution(StageExecutorResult.error());
          stage.incrementImmediateExecutionCount();
        } else {
          stageEntity.setStageState(stageState);
        }
        stageNumber++;
      }

      assertGetDependsOnStagesIsTrue(stages, "STAGE1", Arrays.asList());
      assertGetDependsOnStagesIsTrue(stages, "STAGE2", Arrays.asList());
      assertGetDependsOnStagesIsTrue(stages, "STAGE3", Arrays.asList());
      assertGetDependsOnStagesIsTrue(stages, "STAGE4", Arrays.asList());

      assertGetDependentStagesIsTrue(stages, "STAGE1", Arrays.asList());
      assertGetDependentStagesIsTrue(stages, "STAGE2", Arrays.asList());
      assertGetDependentStagesIsTrue(stages, "STAGE3", Arrays.asList());
      assertGetDependentStagesIsTrue(stages, "STAGE4", Arrays.asList());

      assertIsImmediatelyExecutableStage(stages, process, "STAGE1", false);
      assertIsImmediatelyExecutableStage(stages, process, "STAGE2", true);
      assertIsImmediatelyExecutableStage(stages, process, "STAGE3", true);
      assertIsImmediatelyExecutableStage(stages, process, "STAGE4", true);

      assertIsEventuallyExecutableStage(stages, process, "STAGE1", true);
      assertIsEventuallyExecutableStage(stages, process, "STAGE2", true);
      assertIsEventuallyExecutableStage(stages, process, "STAGE3", true);
      assertIsEventuallyExecutableStage(stages, process, "STAGE4", true);
    }
  }

  @Test
  public void othersActivePendingIndependentOneFailedMaximum() {
    for (StageState stageState : EnumSet.of(ACTIVE, PENDING)) {
      ProcessBuilder builder =
          new ProcessBuilder(UniqueStringGenerator.randomProcessId(DependencyResolverTest.class));
      Process process =
          builder
              .execute("STAGE1")
              .withCallExecutor(
                  ERROR, ExecutorParameters.builder().maximumRetries(0).immediateRetries(0).build())
              .execute("STAGE2")
              .withCallExecutor()
              .execute("STAGE3")
              .withCallExecutor()
              .execute("STAGE4")
              .withCallExecutor()
              .build();
      List<Stage> stages = process.getStages();
      int stageNumber = 0;
      for (Stage stage : stages) {
        StageEntity stageEntity = new StageEntity();
        stage.setStageEntity(stageEntity);
        if (stageNumber == 0) {
          stageEntity.startExecution(stage);
          stageEntity.endExecution(StageExecutorResult.error());
          stage.incrementImmediateExecutionCount();
        } else {
          stageEntity.setStageState(stageState);
        }
        stageNumber++;
      }
      assertGetDependsOnStagesIsTrue(stages, "STAGE1", Arrays.asList());
      assertGetDependsOnStagesIsTrue(stages, "STAGE2", Arrays.asList());
      assertGetDependsOnStagesIsTrue(stages, "STAGE3", Arrays.asList());
      assertGetDependsOnStagesIsTrue(stages, "STAGE4", Arrays.asList());

      assertGetDependentStagesIsTrue(stages, "STAGE1", Arrays.asList());
      assertGetDependentStagesIsTrue(stages, "STAGE2", Arrays.asList());
      assertGetDependentStagesIsTrue(stages, "STAGE3", Arrays.asList());
      assertGetDependentStagesIsTrue(stages, "STAGE4", Arrays.asList());

      assertIsImmediatelyExecutableStage(stages, process, "STAGE1", false);
      assertIsImmediatelyExecutableStage(stages, process, "STAGE2", true);
      assertIsImmediatelyExecutableStage(stages, process, "STAGE3", true);
      assertIsImmediatelyExecutableStage(stages, process, "STAGE4", true);

      assertIsEventuallyExecutableStage(stages, process, "STAGE1", false);
      assertIsEventuallyExecutableStage(stages, process, "STAGE2", true);
      assertIsEventuallyExecutableStage(stages, process, "STAGE3", true);
      assertIsEventuallyExecutableStage(stages, process, "STAGE4", true);
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
              .withCallExecutor()
              .executeAfterPrevious("STAGE2")
              .withCallExecutor()
              .executeAfterPrevious("STAGE3")
              .withCallExecutor()
              .executeAfterPrevious("STAGE4")
              .withCallExecutor()
              .build();
      List<Stage> stages = process.getStages();
      for (Stage stage : stages) {
        stage.setStageEntity(new StageEntity());
        stage.getStageEntity().setStageState(stageState);
      }
      assertGetDependsOnStagesIsTrue(stages, "STAGE1", Arrays.asList());
      assertGetDependsOnStagesIsTrue(stages, "STAGE2", Arrays.asList("STAGE1"));
      assertGetDependsOnStagesIsTrue(stages, "STAGE3", Arrays.asList("STAGE1", "STAGE2"));
      assertGetDependsOnStagesIsTrue(stages, "STAGE4", Arrays.asList("STAGE1", "STAGE2", "STAGE3"));

      assertGetDependentStagesIsTrue(stages, "STAGE1", Arrays.asList("STAGE2", "STAGE3", "STAGE4"));
      assertGetDependentStagesIsTrue(stages, "STAGE2", Arrays.asList("STAGE3", "STAGE4"));
      assertGetDependentStagesIsTrue(stages, "STAGE3", Arrays.asList("STAGE4"));
      assertGetDependentStagesIsTrue(stages, "STAGE4", Arrays.asList());

      assertIsImmediatelyExecutableStage(stages, process, "STAGE1", true);
      assertIsImmediatelyExecutableStage(stages, process, "STAGE2", false);
      assertIsImmediatelyExecutableStage(stages, process, "STAGE3", false);
      assertIsImmediatelyExecutableStage(stages, process, "STAGE4", false);

      assertIsEventuallyExecutableStage(stages, process, "STAGE1", true);
      assertIsEventuallyExecutableStage(stages, process, "STAGE2", true);
      assertIsEventuallyExecutableStage(stages, process, "STAGE3", true);
      assertIsEventuallyExecutableStage(stages, process, "STAGE4", true);
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
              .withCallExecutor()
              .executeAfter("STAGE2", "STAGE1")
              .withCallExecutor()
              .executeAfter("STAGE3", Arrays.asList("STAGE2"))
              .withCallExecutor()
              .executeAfter("STAGE4", Arrays.asList("STAGE3", "STAGE3"))
              .withCallExecutor()
              .build();
      List<Stage> stages = process.getStages();
      for (Stage stage : stages) {
        stage.setStageEntity(new StageEntity());
        stage.getStageEntity().setStageState(stageState);
      }
      assertGetDependsOnStagesIsTrue(stages, "STAGE1", Arrays.asList());
      assertGetDependsOnStagesIsTrue(stages, "STAGE2", Arrays.asList("STAGE1"));
      assertGetDependsOnStagesIsTrue(stages, "STAGE3", Arrays.asList("STAGE1", "STAGE2"));
      assertGetDependsOnStagesIsTrue(stages, "STAGE4", Arrays.asList("STAGE1", "STAGE2", "STAGE3"));

      assertGetDependentStagesIsTrue(stages, "STAGE1", Arrays.asList("STAGE2", "STAGE3", "STAGE4"));
      assertGetDependentStagesIsTrue(stages, "STAGE2", Arrays.asList("STAGE3", "STAGE4"));
      assertGetDependentStagesIsTrue(stages, "STAGE3", Arrays.asList("STAGE4"));
      assertGetDependentStagesIsTrue(stages, "STAGE4", Arrays.asList());

      assertIsImmediatelyExecutableStage(stages, process, "STAGE1", true);
      assertIsImmediatelyExecutableStage(stages, process, "STAGE2", false);
      assertIsImmediatelyExecutableStage(stages, process, "STAGE3", false);
      assertIsImmediatelyExecutableStage(stages, process, "STAGE4", false);

      assertIsEventuallyExecutableStage(stages, process, "STAGE1", true);
      assertIsEventuallyExecutableStage(stages, process, "STAGE2", true);
      assertIsEventuallyExecutableStage(stages, process, "STAGE3", true);
      assertIsEventuallyExecutableStage(stages, process, "STAGE4", true);
    }
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

    CallExecutor executor = new CallExecutor(StageState.SUCCESS);
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
      List<Stage> stages, Process process, String stageName, boolean expected) {
    assertIsImmediatelyExecutableStage(
        stages, getStageByStageName(process.getStages(), stageName), expected);
  }

  private void assertIsImmediatelyExecutableStage(
      List<Stage> stages, Stage stage, boolean expected) {
    assertThat(isImmediatelyExecutableStage(stages, Collections.emptySet(), stage))
        .isEqualTo(expected);
  }

  private void assertIsEventuallyExecutableStage(
      List<Stage> stages, Process process, String stageName, boolean expected) {
    assertIsEventuallyExecutableStage(
        stages, getStageByStageName(process.getStages(), stageName), expected);
  }

  private void assertIsEventuallyExecutableStage(
      List<Stage> stages, Stage stage, boolean expected) {
    assertThat(isEventuallyExecutableStage(stages, stage)).isEqualTo(expected);
  }

  private void assertGetDependentStagesIsTrue(
      List<Stage> stages, String stageName, List<String> expectedDependentStageNames) {
    List<Stage> dependentStages =
        DependencyResolver.getDependentStages(stages, getStageByStageName(stages, stageName));
    List<String> dependentStageNames =
        dependentStages.stream().map(s -> s.getStageName()).collect(Collectors.toList());
    assertThat(expectedDependentStageNames)
        .containsExactlyInAnyOrderElementsOf(dependentStageNames);
  }

  private void assertGetDependsOnStagesIsTrue(
      List<Stage> stages, String stageName, List<String> expectedDependsOnStageNames) {
    List<Stage> dependsOnStages =
        DependencyResolver.getDependsOnStages(stages, getStageByStageName(stages, stageName));
    List<String> dependsOnStageNames =
        dependsOnStages.stream().map(s -> s.getStageName()).collect(Collectors.toList());
    assertThat(expectedDependsOnStageNames)
        .containsExactlyInAnyOrderElementsOf(dependsOnStageNames);
  }

  private Stage getStageByStageName(List<Stage> stages, String stageName) {
    for (Stage stage : stages) {
      if (stage.getStageName().equals(stageName)) {
        return stage;
      }
    }
    return null;
  }
}
