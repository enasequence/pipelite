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
import static pipelite.stage.StageState.*;

import java.util.*;
import java.util.stream.Collectors;
import lombok.Value;
import lombok.experimental.Accessors;
import org.junit.jupiter.api.Test;
import pipelite.PipeliteIdCreator;
import pipelite.entity.StageEntity;
import pipelite.executor.SyncTestExecutor;
import pipelite.process.Process;
import pipelite.process.builder.ProcessBuilder;
import pipelite.process.builder.ProcessBuilderHelper;
import pipelite.stage.Stage;
import pipelite.stage.StageState;
import pipelite.stage.executor.ErrorType;
import pipelite.stage.executor.StageExecutor;
import pipelite.stage.executor.StageExecutorResult;
import pipelite.stage.executor.StageExecutorState;
import pipelite.stage.parameters.ExecutorParameters;

public class DependencyResolverTest {

  private static final ExecutorParameters NO_RETRIES_EXECUTOR_PARAMS =
      ExecutorParameters.builder().maximumRetries(0).immediateRetries(0).build();

  private ProcessBuilder createProcessBuilder() {
    return new ProcessBuilder(PipeliteIdCreator.processId());
  }

  @Test
  public void allActivePendingDependOnPrevious() {
    for (StageState stageState : EnumSet.of(ACTIVE, PENDING)) {

      ProcessBuilder builder = createProcessBuilder();
      Process process =
          builder
              .execute("STAGE1")
              .withSyncTestExecutor()
              .executeAfterPrevious("STAGE2")
              .withSyncTestExecutor()
              .executeAfterPrevious("STAGE3")
              .withSyncTestExecutor()
              .build();
      Set<Stage> stages = process.getStages();
      for (Stage stage : process.getStages()) {
        simulateStageExecution(stage, stageState);
      }

      assertGetDependsOnStages(process, "STAGE1", Collections.emptyList());
      assertGetDependsOnStages(process, "STAGE2", Arrays.asList("STAGE1"));
      assertGetDependsOnStages(process, "STAGE3", Arrays.asList("STAGE1", "STAGE2"));

      assertGetDependsOnStagesDirectly(process, "STAGE1", Collections.emptyList());
      assertGetDependsOnStagesDirectly(process, "STAGE2", Arrays.asList("STAGE1"));
      assertGetDependsOnStagesDirectly(process, "STAGE3", Arrays.asList("STAGE2"));

      assertGetDependentStages(process, "STAGE1", Arrays.asList("STAGE2", "STAGE3"));
      assertGetDependentStages(process, "STAGE2", Arrays.asList("STAGE3"));
      assertGetDependentStages(process, "STAGE3", Collections.emptyList());

      assertGetPermanentFailedStages(stages, Collections.emptyList());
      assertGetImmediatelyExecutableStages(process, Arrays.asList("STAGE1"));
      assertGetEventuallyExecutableStages(process, Arrays.asList("STAGE1", "STAGE2", "STAGE3"));
    }
  }

  @Test
  public void allActivePendingOthersDependOnFirstAllActive() {
    for (StageState stageState : EnumSet.of(ACTIVE, PENDING)) {
      ProcessBuilder builder = createProcessBuilder();
      Process process =
          builder
              .execute("STAGE1")
              .withSyncTestExecutor()
              .executeAfter("STAGE2", "STAGE1")
              .withSyncTestExecutor()
              .executeAfter("STAGE3", "STAGE1")
              .withSyncTestExecutor()
              .build();
      Set<Stage> stages = process.getStages();
      for (Stage stage : process.getStages()) {
        simulateStageExecution(stage, stageState);
      }

      assertGetDependsOnStages(process, "STAGE1", Collections.emptyList());
      assertGetDependsOnStages(process, "STAGE2", Arrays.asList("STAGE1"));
      assertGetDependsOnStages(process, "STAGE3", Arrays.asList("STAGE1"));

      assertGetDependsOnStagesDirectly(process, "STAGE1", Collections.emptyList());
      assertGetDependsOnStagesDirectly(process, "STAGE2", Arrays.asList("STAGE1"));
      assertGetDependsOnStagesDirectly(process, "STAGE3", Arrays.asList("STAGE1"));

      assertGetDependentStages(process, "STAGE1", Arrays.asList("STAGE2", "STAGE3"));
      assertGetDependentStages(process, "STAGE2", Collections.emptyList());
      assertGetDependentStages(process, "STAGE3", Collections.emptyList());

      assertGetPermanentFailedStages(stages, Collections.emptyList());
      assertGetImmediatelyExecutableStages(process, Arrays.asList("STAGE1"));
      assertGetEventuallyExecutableStages(process, Arrays.asList("STAGE1", "STAGE2", "STAGE3"));
    }
  }

  @Test
  public void othersActivePendingDependOnFirstThatSucceeded() {
    for (StageState stageState : EnumSet.of(ACTIVE, PENDING)) {

      ProcessBuilder builder = createProcessBuilder();
      Process process =
          builder
              .execute("STAGE1")
              .withSyncTestExecutor()
              .executeAfter("STAGE2", "STAGE1")
              .withSyncTestExecutor()
              .executeAfter("STAGE3", "STAGE1")
              .withSyncTestExecutor()
              .build();
      Set<Stage> stages = process.getStages();
      int stageNumber = 0;
      for (Stage stage : process.getStages()) {
        if (stageNumber == 0) {
          simulateStageExecution(stage, StageState.SUCCESS);
        } else {
          simulateStageExecution(stage, stageState);
        }
        stageNumber++;
      }

      assertGetDependsOnStages(process, "STAGE1", Collections.emptyList());
      assertGetDependsOnStages(process, "STAGE2", Arrays.asList("STAGE1"));
      assertGetDependsOnStages(process, "STAGE3", Arrays.asList("STAGE1"));

      assertGetDependsOnStagesDirectly(process, "STAGE1", Collections.emptyList());
      assertGetDependsOnStagesDirectly(process, "STAGE2", Arrays.asList("STAGE1"));
      assertGetDependsOnStagesDirectly(process, "STAGE3", Arrays.asList("STAGE1"));

      assertGetDependentStages(process, "STAGE1", Arrays.asList("STAGE2", "STAGE3"));
      assertGetDependentStages(process, "STAGE2", Collections.emptyList());
      assertGetDependentStages(process, "STAGE3", Collections.emptyList());

      assertGetPermanentFailedStages(stages, Collections.emptyList());
      assertGetImmediatelyExecutableStages(process, Arrays.asList("STAGE2", "STAGE3"));
      assertGetEventuallyExecutableStages(process, Arrays.asList("STAGE2", "STAGE3"));
    }
  }

  @Test
  public void othersActivePendingDependOnFirstInErrorNoRetriesLeft() {
    for (StageState stageState : EnumSet.of(ACTIVE, PENDING)) {

      ProcessBuilder builder = createProcessBuilder();
      Process process =
          builder
              .execute("STAGE1")
              .withSyncTestExecutor(StageExecutorState.ERROR, NO_RETRIES_EXECUTOR_PARAMS)
              .executeAfter("STAGE2", "STAGE1")
              .withSyncTestExecutor()
              .executeAfter("STAGE3", "STAGE1")
              .withSyncTestExecutor()
              .build();
      Set<Stage> stages = process.getStages();
      int stageNumber = 0;
      for (Stage stage : process.getStages()) {
        if (stageNumber == 0) {
          simulateStageExecution(stage, StageState.ERROR);
        } else {
          simulateStageExecution(stage, stageState);
        }
        stageNumber++;
      }

      assertGetDependsOnStages(process, "STAGE1", Collections.emptyList());
      assertGetDependsOnStages(process, "STAGE2", Arrays.asList("STAGE1"));
      assertGetDependsOnStages(process, "STAGE3", Arrays.asList("STAGE1"));

      assertGetDependsOnStagesDirectly(process, "STAGE1", Collections.emptyList());
      assertGetDependsOnStagesDirectly(process, "STAGE2", Arrays.asList("STAGE1"));
      assertGetDependsOnStagesDirectly(process, "STAGE3", Arrays.asList("STAGE1"));

      assertGetDependentStages(process, "STAGE1", Arrays.asList("STAGE2", "STAGE3"));
      assertGetDependentStages(process, "STAGE2", Collections.emptyList());
      assertGetDependentStages(process, "STAGE3", Collections.emptyList());

      assertGetPermanentFailedStages(stages, Arrays.asList("STAGE1"));
      assertGetImmediatelyExecutableStages(process, Collections.emptyList());
      assertGetEventuallyExecutableStages(process, Collections.emptyList());
    }
  }

  @Test
  public void othersActivePendingDependOnFirstInErrorMaxRetriesLeft() {
    for (StageState stageState : EnumSet.of(ACTIVE, PENDING)) {

      ProcessBuilder builder = createProcessBuilder();
      Process process =
          builder
              .execute("STAGE1")
              .withSyncTestExecutor(
                  StageExecutorState.ERROR,
                  ExecutorParameters.builder().maximumRetries(3).immediateRetries(0).build())
              .executeAfter("STAGE2", "STAGE1")
              .withSyncTestExecutor()
              .executeAfter("STAGE3", "STAGE1")
              .withSyncTestExecutor()
              .build();
      Set<Stage> stages = process.getStages();
      int stageNumber = 0;
      for (Stage stage : process.getStages()) {
        if (stageNumber == 0) {
          simulateStageExecution(stage, StageState.ERROR);
        } else {
          simulateStageExecution(stage, stageState);
        }
        stageNumber++;
      }

      assertGetDependsOnStages(process, "STAGE1", Collections.emptyList());
      assertGetDependsOnStages(process, "STAGE2", Arrays.asList("STAGE1"));
      assertGetDependsOnStages(process, "STAGE3", Arrays.asList("STAGE1"));

      assertGetDependsOnStagesDirectly(process, "STAGE1", Collections.emptyList());
      assertGetDependsOnStagesDirectly(process, "STAGE2", Arrays.asList("STAGE1"));
      assertGetDependsOnStagesDirectly(process, "STAGE3", Arrays.asList("STAGE1"));

      assertGetDependentStages(process, "STAGE1", Arrays.asList("STAGE2", "STAGE3"));
      assertGetDependentStages(process, "STAGE2", Collections.emptyList());
      assertGetDependentStages(process, "STAGE3", Collections.emptyList());

      assertGetPermanentFailedStages(stages, Collections.emptyList());
      assertGetImmediatelyExecutableStages(process, Collections.emptyList());
      assertGetEventuallyExecutableStages(process, Arrays.asList("STAGE1", "STAGE2", "STAGE3"));
    }
  }

  @Test
  public void othersActivePendingDependOnFirstInPermanentError() {
    for (StageState stageState : EnumSet.of(ACTIVE, PENDING)) {
      ProcessBuilder builder = createProcessBuilder();
      Process process =
          builder
              .execute("STAGE1")
              .withSyncTestExecutor(StageExecutorState.ERROR)
              .executeAfter("STAGE2", "STAGE1")
              .withSyncTestExecutor()
              .executeAfter("STAGE3", "STAGE1")
              .withSyncTestExecutor()
              .build();
      Set<Stage> stages = process.getStages();
      int stageNumber = 0;
      for (Stage stage : process.getStages()) {
        if (stageNumber == 0) {
          simulateStageExecution(stage, ErrorType.PERMANENT_ERROR);
        } else {
          simulateStageExecution(stage, stageState);
        }
        stageNumber++;
      }

      assertGetDependsOnStages(process, "STAGE1", Collections.emptyList());
      assertGetDependsOnStages(process, "STAGE2", Arrays.asList("STAGE1"));
      assertGetDependsOnStages(process, "STAGE3", Arrays.asList("STAGE1"));

      assertGetDependsOnStagesDirectly(process, "STAGE1", Collections.emptyList());
      assertGetDependsOnStagesDirectly(process, "STAGE2", Arrays.asList("STAGE1"));
      assertGetDependsOnStagesDirectly(process, "STAGE3", Arrays.asList("STAGE1"));

      assertGetDependentStages(process, "STAGE1", Arrays.asList("STAGE2", "STAGE3"));
      assertGetDependentStages(process, "STAGE2", Collections.emptyList());
      assertGetDependentStages(process, "STAGE3", Collections.emptyList());

      assertGetPermanentFailedStages(stages, Arrays.asList("STAGE1"));
      assertGetImmediatelyExecutableStages(process, Collections.emptyList());
      assertGetEventuallyExecutableStages(process, Collections.emptyList());
    }
  }

  @Test
  public void othersActivePendingIndependentOneErrorMaxAndImmediateRetriesLeft() {
    for (StageState stageState : EnumSet.of(ACTIVE, PENDING)) {
      ProcessBuilder builder = createProcessBuilder();
      Process process =
          builder
              .execute("STAGE1")
              .withSyncTestExecutor(
                  StageExecutorState.ERROR,
                  ExecutorParameters.builder().maximumRetries(1).immediateRetries(1).build())
              .execute("STAGE2")
              .withSyncTestExecutor()
              .execute("STAGE3")
              .withSyncTestExecutor()
              .build();
      Set<Stage> stages = process.getStages();
      int stageNumber = 0;
      for (Stage stage : process.getStages()) {
        if (stageNumber == 0) {
          simulateStageExecution(stage, StageState.ERROR);
        } else {
          simulateStageExecution(stage, stageState);
        }
        stageNumber++;
      }

      assertGetDependsOnStages(process, "STAGE1", Collections.emptyList());
      assertGetDependsOnStages(process, "STAGE2", Collections.emptyList());
      assertGetDependsOnStages(process, "STAGE3", Collections.emptyList());

      assertGetDependsOnStagesDirectly(process, "STAGE1", Collections.emptyList());
      assertGetDependsOnStagesDirectly(process, "STAGE2", Collections.emptyList());
      assertGetDependsOnStagesDirectly(process, "STAGE3", Collections.emptyList());

      assertGetDependentStages(process, "STAGE1", Collections.emptyList());
      assertGetDependentStages(process, "STAGE2", Collections.emptyList());
      assertGetDependentStages(process, "STAGE3", Collections.emptyList());

      assertGetPermanentFailedStages(stages, Collections.emptyList());
      assertGetImmediatelyExecutableStages(process, Arrays.asList("STAGE1", "STAGE2", "STAGE3"));
      assertGetEventuallyExecutableStages(process, Arrays.asList("STAGE1", "STAGE2", "STAGE3"));
    }
  }

  @Test
  public void othersActivePendingIndependentOneErrorMaxRetriesLeft() {
    for (StageState stageState : EnumSet.of(ACTIVE, PENDING)) {
      ProcessBuilder builder = createProcessBuilder();
      Process process =
          builder
              .execute("STAGE1")
              .withSyncTestExecutor(
                  StageExecutorState.ERROR,
                  ExecutorParameters.builder().maximumRetries(1).immediateRetries(0).build())
              .execute("STAGE2")
              .withSyncTestExecutor()
              .execute("STAGE3")
              .withSyncTestExecutor()
              .execute("STAGE4")
              .withSyncTestExecutor()
              .build();
      Set<Stage> stages = process.getStages();
      int stageNumber = 0;
      for (Stage stage : process.getStages()) {
        if (stageNumber == 0) {
          simulateStageExecution(stage, StageState.ERROR);
        } else {
          simulateStageExecution(stage, stageState);
        }
        stageNumber++;
      }

      assertGetDependsOnStages(process, "STAGE1", Collections.emptyList());
      assertGetDependsOnStages(process, "STAGE2", Collections.emptyList());
      assertGetDependsOnStages(process, "STAGE3", Collections.emptyList());
      assertGetDependsOnStages(process, "STAGE4", Collections.emptyList());

      assertGetDependsOnStagesDirectly(process, "STAGE1", Collections.emptyList());
      assertGetDependsOnStagesDirectly(process, "STAGE2", Collections.emptyList());
      assertGetDependsOnStagesDirectly(process, "STAGE3", Collections.emptyList());
      assertGetDependsOnStagesDirectly(process, "STAGE4", Collections.emptyList());

      assertGetDependentStages(process, "STAGE1", Collections.emptyList());
      assertGetDependentStages(process, "STAGE2", Collections.emptyList());
      assertGetDependentStages(process, "STAGE3", Collections.emptyList());
      assertGetDependentStages(process, "STAGE4", Collections.emptyList());

      assertGetPermanentFailedStages(stages, Collections.emptyList());
      assertGetImmediatelyExecutableStages(process, Arrays.asList("STAGE2", "STAGE3", "STAGE4"));
      assertGetEventuallyExecutableStages(
          process, Arrays.asList("STAGE1", "STAGE2", "STAGE3", "STAGE4"));
    }
  }

  @Test
  public void othersActivePendingIndependentOneErrorNoRetriesLeft() {
    for (StageState stageState : EnumSet.of(ACTIVE, PENDING)) {
      ProcessBuilder builder = createProcessBuilder();
      Process process =
          builder
              .execute("STAGE1")
              .withSyncTestExecutor(
                  StageExecutorState.ERROR,
                  ExecutorParameters.builder().maximumRetries(0).immediateRetries(0).build())
              .execute("STAGE2")
              .withSyncTestExecutor()
              .execute("STAGE3")
              .withSyncTestExecutor()
              .execute("STAGE4")
              .withSyncTestExecutor()
              .build();
      Set<Stage> stages = process.getStages();
      int stageNumber = 0;
      for (Stage stage : stages) {
        if (stageNumber == 0) {
          simulateStageExecution(stage, StageState.ERROR);
        } else {
          simulateStageExecution(stage, stageState);
        }
        stageNumber++;
      }
      assertGetDependsOnStages(process, "STAGE1", Collections.emptyList());
      assertGetDependsOnStages(process, "STAGE2", Collections.emptyList());
      assertGetDependsOnStages(process, "STAGE3", Collections.emptyList());
      assertGetDependsOnStages(process, "STAGE4", Collections.emptyList());

      assertGetDependsOnStagesDirectly(process, "STAGE1", Collections.emptyList());
      assertGetDependsOnStagesDirectly(process, "STAGE2", Collections.emptyList());
      assertGetDependsOnStagesDirectly(process, "STAGE3", Collections.emptyList());
      assertGetDependsOnStagesDirectly(process, "STAGE4", Collections.emptyList());

      assertGetDependentStages(process, "STAGE1", Collections.emptyList());
      assertGetDependentStages(process, "STAGE2", Collections.emptyList());
      assertGetDependentStages(process, "STAGE3", Collections.emptyList());
      assertGetDependentStages(process, "STAGE4", Collections.emptyList());

      assertGetPermanentFailedStages(stages, Arrays.asList("STAGE1"));
      assertGetImmediatelyExecutableStages(process, Arrays.asList("STAGE2", "STAGE3", "STAGE4"));
      assertGetEventuallyExecutableStages(process, Arrays.asList("STAGE2", "STAGE3", "STAGE4"));
    }
  }

  @Test
  public void othersActivePendingIndependentOnePermanentError() {
    for (StageState stageState : EnumSet.of(ACTIVE, PENDING)) {
      ProcessBuilder builder = createProcessBuilder();
      Process process =
          builder
              .execute("STAGE1")
              .withSyncTestExecutor(StageExecutorState.ERROR)
              .execute("STAGE2")
              .withSyncTestExecutor()
              .execute("STAGE3")
              .withSyncTestExecutor()
              .execute("STAGE4")
              .withSyncTestExecutor()
              .build();
      Set<Stage> stages = process.getStages();
      int stageNumber = 0;
      for (Stage stage : stages) {
        if (stageNumber == 0) {
          simulateStageExecution(stage, ErrorType.PERMANENT_ERROR);
        } else {
          simulateStageExecution(stage, stageState);
        }
        stageNumber++;
      }
      assertGetDependsOnStages(process, "STAGE1", Collections.emptyList());
      assertGetDependsOnStages(process, "STAGE2", Collections.emptyList());
      assertGetDependsOnStages(process, "STAGE3", Collections.emptyList());
      assertGetDependsOnStages(process, "STAGE4", Collections.emptyList());

      assertGetDependsOnStagesDirectly(process, "STAGE1", Collections.emptyList());
      assertGetDependsOnStagesDirectly(process, "STAGE2", Collections.emptyList());
      assertGetDependsOnStagesDirectly(process, "STAGE3", Collections.emptyList());
      assertGetDependsOnStagesDirectly(process, "STAGE4", Collections.emptyList());

      assertGetDependentStages(process, "STAGE1", Collections.emptyList());
      assertGetDependentStages(process, "STAGE2", Collections.emptyList());
      assertGetDependentStages(process, "STAGE3", Collections.emptyList());
      assertGetDependentStages(process, "STAGE4", Collections.emptyList());

      assertGetPermanentFailedStages(stages, Arrays.asList("STAGE1"));
      assertGetImmediatelyExecutableStages(process, Arrays.asList("STAGE2", "STAGE3", "STAGE4"));
      assertGetEventuallyExecutableStages(process, Arrays.asList("STAGE2", "STAGE3", "STAGE4"));
    }
  }

  @Test
  public void allActivePendingOthersDependOnPrevious() {
    for (StageState stageState : EnumSet.of(ACTIVE, PENDING)) {
      ProcessBuilder builder = createProcessBuilder();
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
      Set<Stage> stages = process.getStages();
      for (Stage stage : stages) {
        stage.setStageEntity(new StageEntity());
        stage.getStageEntity().setStageState(stageState);
      }
      assertGetDependsOnStages(process, "STAGE1", Collections.emptyList());
      assertGetDependsOnStages(process, "STAGE2", Arrays.asList("STAGE1"));
      assertGetDependsOnStages(process, "STAGE3", Arrays.asList("STAGE1", "STAGE2"));
      assertGetDependsOnStages(process, "STAGE4", Arrays.asList("STAGE1", "STAGE2", "STAGE3"));

      assertGetDependsOnStagesDirectly(process, "STAGE1", Collections.emptyList());
      assertGetDependsOnStagesDirectly(process, "STAGE2", Arrays.asList("STAGE1"));
      assertGetDependsOnStagesDirectly(process, "STAGE3", Arrays.asList("STAGE2"));
      assertGetDependsOnStagesDirectly(process, "STAGE4", Arrays.asList("STAGE3"));

      assertGetDependentStages(process, "STAGE1", Arrays.asList("STAGE2", "STAGE3", "STAGE4"));
      assertGetDependentStages(process, "STAGE2", Arrays.asList("STAGE3", "STAGE4"));
      assertGetDependentStages(process, "STAGE3", Arrays.asList("STAGE4"));
      assertGetDependentStages(process, "STAGE4", Collections.emptyList());

      assertGetPermanentFailedStages(stages, Collections.emptyList());
      assertGetImmediatelyExecutableStages(process, Arrays.asList("STAGE1"));
      assertGetEventuallyExecutableStages(
          process, Arrays.asList("STAGE1", "STAGE2", "STAGE3", "STAGE4"));
    }
  }

  @Test
  public void allActivePendingOthersDependOnFirstTransitively() {
    for (StageState stageState : EnumSet.of(ACTIVE, PENDING)) {
      ProcessBuilder builder = createProcessBuilder();
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
      Set<Stage> stages = process.getStages();
      for (Stage stage : stages) {
        stage.setStageEntity(new StageEntity());
        stage.getStageEntity().setStageState(stageState);
      }
      assertGetDependsOnStages(process, "STAGE1", Collections.emptyList());
      assertGetDependsOnStages(process, "STAGE2", Arrays.asList("STAGE1"));
      assertGetDependsOnStages(process, "STAGE3", Arrays.asList("STAGE1", "STAGE2"));
      assertGetDependsOnStages(process, "STAGE4", Arrays.asList("STAGE1", "STAGE2", "STAGE3"));

      assertGetDependsOnStagesDirectly(process, "STAGE1", Collections.emptyList());
      assertGetDependsOnStagesDirectly(process, "STAGE2", Arrays.asList("STAGE1"));
      assertGetDependsOnStagesDirectly(process, "STAGE3", Arrays.asList("STAGE2"));
      assertGetDependsOnStagesDirectly(process, "STAGE4", Arrays.asList("STAGE3"));

      assertGetDependentStages(process, "STAGE1", Arrays.asList("STAGE2", "STAGE3", "STAGE4"));
      assertGetDependentStages(process, "STAGE2", Arrays.asList("STAGE3", "STAGE4"));
      assertGetDependentStages(process, "STAGE3", Arrays.asList("STAGE4"));
      assertGetDependentStages(process, "STAGE4", Collections.emptyList());

      assertGetPermanentFailedStages(stages, Collections.emptyList());
      assertGetImmediatelyExecutableStages(process, Arrays.asList("STAGE1"));
      assertGetEventuallyExecutableStages(
          process, Arrays.asList("STAGE1", "STAGE2", "STAGE3", "STAGE4"));
    }
  }

  @Test
  public void independentAllErrorNoRetriesLeft() {
    ProcessBuilder builder = createProcessBuilder();
    Process process =
        builder
            .execute("STAGE1")
            .withSyncTestExecutor(StageExecutorState.ERROR, NO_RETRIES_EXECUTOR_PARAMS)
            .execute("STAGE2")
            .withSyncTestExecutor(StageExecutorState.ERROR, NO_RETRIES_EXECUTOR_PARAMS)
            .execute("STAGE3")
            .withSyncTestExecutor(StageExecutorState.ERROR, NO_RETRIES_EXECUTOR_PARAMS)
            .execute("STAGE4")
            .withSyncTestExecutor(StageExecutorState.ERROR, NO_RETRIES_EXECUTOR_PARAMS)
            .build();
    Set<Stage> stages = process.getStages();
    for (Stage stage : stages) {
      simulateStageExecution(stage, StageState.ERROR);
    }
    assertGetDependsOnStages(process, "STAGE1", Collections.emptyList());
    assertGetDependsOnStages(process, "STAGE2", Collections.emptyList());
    assertGetDependsOnStages(process, "STAGE3", Collections.emptyList());
    assertGetDependsOnStages(process, "STAGE4", Collections.emptyList());

    assertGetDependsOnStagesDirectly(process, "STAGE1", Collections.emptyList());
    assertGetDependsOnStagesDirectly(process, "STAGE2", Collections.emptyList());
    assertGetDependsOnStagesDirectly(process, "STAGE3", Collections.emptyList());
    assertGetDependsOnStagesDirectly(process, "STAGE4", Collections.emptyList());

    assertGetDependentStages(process, "STAGE1", Collections.emptyList());
    assertGetDependentStages(process, "STAGE2", Collections.emptyList());
    assertGetDependentStages(process, "STAGE3", Collections.emptyList());
    assertGetDependentStages(process, "STAGE4", Collections.emptyList());

    assertGetPermanentFailedStages(stages, Arrays.asList("STAGE1", "STAGE2", "STAGE3", "STAGE4"));
    assertGetImmediatelyExecutableStages(process, Collections.emptyList());
    assertGetEventuallyExecutableStages(process, Collections.emptyList());
  }

  @Test
  public void independentAllPermanentError() {
    ProcessBuilder builder = createProcessBuilder();
    Process process =
        builder
            .execute("STAGE1")
            .withSyncTestExecutor(StageExecutorState.ERROR)
            .execute("STAGE2")
            .withSyncTestExecutor(StageExecutorState.ERROR)
            .execute("STAGE3")
            .withSyncTestExecutor(StageExecutorState.ERROR)
            .execute("STAGE4")
            .withSyncTestExecutor(StageExecutorState.ERROR)
            .build();
    Set<Stage> stages = process.getStages();
    for (Stage stage : stages) {
      simulateStageExecution(stage, ErrorType.PERMANENT_ERROR);
    }
    assertGetDependsOnStages(process, "STAGE1", Collections.emptyList());
    assertGetDependsOnStages(process, "STAGE2", Collections.emptyList());
    assertGetDependsOnStages(process, "STAGE3", Collections.emptyList());
    assertGetDependsOnStages(process, "STAGE4", Collections.emptyList());

    assertGetDependsOnStagesDirectly(process, "STAGE1", Collections.emptyList());
    assertGetDependsOnStagesDirectly(process, "STAGE2", Collections.emptyList());
    assertGetDependsOnStagesDirectly(process, "STAGE3", Collections.emptyList());
    assertGetDependsOnStagesDirectly(process, "STAGE4", Collections.emptyList());

    assertGetDependentStages(process, "STAGE1", Collections.emptyList());
    assertGetDependentStages(process, "STAGE2", Collections.emptyList());
    assertGetDependentStages(process, "STAGE3", Collections.emptyList());
    assertGetDependentStages(process, "STAGE4", Collections.emptyList());

    assertGetPermanentFailedStages(stages, Arrays.asList("STAGE1", "STAGE2", "STAGE3", "STAGE4"));
    assertGetImmediatelyExecutableStages(process, Collections.emptyList());
    assertGetEventuallyExecutableStages(process, Collections.emptyList());
  }

  @Test
  public void isImmediatelyExecutableStage() {
    final int MAX_EXEC_CNT = 3;
    for (int execCnt = 0; execCnt < MAX_EXEC_CNT; execCnt++) {
      for (int immCnt = 0; immCnt < MAX_EXEC_CNT; immCnt++) {
        for (int maxRetry = 0; maxRetry < MAX_EXEC_CNT; maxRetry++) {
          for (int immRetry = 0; immRetry < MAX_EXEC_CNT; immRetry++) {
            assertThat(isImmediatelyExecutableStage(PENDING, execCnt, immCnt, maxRetry, immRetry))
                .isTrue();
            assertThat(isImmediatelyExecutableStage(ACTIVE, execCnt, immCnt, maxRetry, immRetry))
                .isTrue();
            assertThat(isImmediatelyExecutableStage(ERROR, execCnt, immCnt, maxRetry, immRetry))
                .isEqualTo(execCnt <= maxRetry && immCnt <= Math.min(immRetry, maxRetry));
            assertThat(isImmediatelyExecutableStage(SUCCESS, execCnt, immCnt, maxRetry, immRetry))
                .isFalse();
            assertThat(
                    isImmediatelyExecutableStage(
                        ErrorType.PERMANENT_ERROR, execCnt, immCnt, maxRetry, immRetry))
                .isFalse();
          }
        }
      }
    }
  }

  @Test
  public void isEventuallyExecutableStage() {
    final int MAX_EXEC_CNT = 3;
    for (int execCnt = 0; execCnt < MAX_EXEC_CNT; execCnt++) {
      for (int immCnt = 0; immCnt < MAX_EXEC_CNT; immCnt++) {
        for (int maxRetry = 0; maxRetry < MAX_EXEC_CNT; maxRetry++) {
          for (int immRetry = 0; immRetry < MAX_EXEC_CNT; immRetry++) {
            assertThat(isEventuallyExecutableStage(PENDING, execCnt, immCnt, maxRetry, immRetry))
                .isTrue();
            assertThat(isEventuallyExecutableStage(ACTIVE, execCnt, immCnt, maxRetry, immRetry))
                .isTrue();
            assertThat(isEventuallyExecutableStage(ERROR, execCnt, immCnt, maxRetry, immRetry))
                .isEqualTo(execCnt <= maxRetry);
            assertThat(isEventuallyExecutableStage(SUCCESS, execCnt, immCnt, maxRetry, immRetry))
                .isFalse();
            assertThat(
                    isEventuallyExecutableStage(
                        ErrorType.PERMANENT_ERROR, execCnt, immCnt, maxRetry, immRetry))
                .isFalse();
          }
        }
      }
    }
  }

  private SingleStageProcess simulatedStageExecution(
      StageState stageState,
      int executionCount,
      int immediateExecutionCount,
      int maximumRetries,
      int immediateRetries) {
    StageEntity stageEntity = new StageEntity();
    stageEntity.setExecutionCount(executionCount);
    stageEntity.setStageState(stageState);
    SyncTestExecutor executor =
        StageExecutor.createSyncTestExecutor(StageExecutorState.SUCCESS, null);
    executor.setExecutorParams(
        ExecutorParameters.builder()
            .immediateRetries(immediateRetries)
            .maximumRetries(maximumRetries)
            .build());
    Stage stage = Stage.builder().stageName("STAGE").executor(executor).build();
    stage.setStageEntity(stageEntity);
    for (int i = 0; i < immediateExecutionCount; ++i) {
      stage.incrementImmediateExecutionCount();
    }
    return new SingleStageProcess(
        new Process(
            "TEST",
            ProcessBuilderHelper.stageGraph(
                Arrays.asList(new ProcessBuilderHelper.AddedStage(stage)))),
        stage);
  }

  @Value()
  @Accessors(fluent = true)
  private static class SingleStageProcess {
    private final Process process;
    private final Stage stage;
  }

  private SingleStageProcess simulatedStageExecution(
      ErrorType errorType,
      int executionCount,
      int immediateCount,
      int maximumRetries,
      int immediateRetries) {
    StageEntity stageEntity = new StageEntity();
    stageEntity.setExecutionCount(executionCount);
    stageEntity.setStageState(ERROR);
    stageEntity.setErrorType(errorType);
    SyncTestExecutor executor =
        StageExecutor.createSyncTestExecutor(StageExecutorState.SUCCESS, null);
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
    return new SingleStageProcess(
        new Process(
            "TEST",
            ProcessBuilderHelper.stageGraph(
                Arrays.asList(new ProcessBuilderHelper.AddedStage(stage)))),
        stage);
  }

  private boolean isImmediatelyExecutableStage(
      StageState stageState,
      int executionCount,
      int immediateExecutionCount,
      int maximumRetries,
      int immediateRetries) {
    SingleStageProcess s =
        simulatedStageExecution(
            stageState, executionCount, immediateExecutionCount, maximumRetries, immediateRetries);
    return DependencyResolver.isImmediatelyExecutableStage(
        s.process, Collections.emptySet(), s.stage);
  }

  private boolean isImmediatelyExecutableStage(
      ErrorType errorType,
      int executionCount,
      int immediateExecutionCount,
      int maximumRetries,
      int immediateRetries) {
    SingleStageProcess s =
        simulatedStageExecution(
            errorType, executionCount, immediateExecutionCount, maximumRetries, immediateRetries);
    return DependencyResolver.isImmediatelyExecutableStage(
        s.process, Collections.emptySet(), s.stage);
  }

  private boolean isEventuallyExecutableStage(
      StageState stageState,
      int executionCount,
      int immediateExecutionCount,
      int maximumRetries,
      int immediateRetries) {
    SingleStageProcess s =
        simulatedStageExecution(
            stageState, executionCount, immediateExecutionCount, maximumRetries, immediateRetries);
    return DependencyResolver.isEventuallyExecutableStage(s.process, s.stage);
  }

  private boolean isEventuallyExecutableStage(
      ErrorType errorType,
      int executionCount,
      int immediateExecutionCount,
      int maximumRetries,
      int immediateRetries) {
    SingleStageProcess s =
        simulatedStageExecution(
            errorType, executionCount, immediateExecutionCount, maximumRetries, immediateRetries);
    return DependencyResolver.isEventuallyExecutableStage(s.process, s.stage);
  }

  private void assertGetDependentStages(
      Process process, String stageName, List<String> expectedDependentStageNames) {
    List<Stage> dependentStages =
        DependencyResolver.getDependentStages(process, process.getStage(stageName).get());
    List<String> dependentStageNames =
        dependentStages.stream().map(s -> s.getStageName()).collect(Collectors.toList());
    assertThat(expectedDependentStageNames)
        .containsExactlyInAnyOrderElementsOf(dependentStageNames);
  }

  private void assertGetDependsOnStages(
      Process process, String stageName, List<String> expectedDependsOnStageNames) {
    Set<Stage> dependsOnStages =
        DependencyResolver.getDependsOnStages(process, process.getStage(stageName).get());
    List<String> dependsOnStageNames =
        dependsOnStages.stream().map(s -> s.getStageName()).collect(Collectors.toList());
    assertThat(expectedDependsOnStageNames)
        .containsExactlyInAnyOrderElementsOf(dependsOnStageNames);
  }

  private void assertGetDependsOnStagesDirectly(
      Process process, String stageName, List<String> expectedDependsOnStageNames) {
    Set<Stage> dependsOnStages =
        DependencyResolver.getDependsOnStagesDirectly(process, process.getStage(stageName).get());
    List<String> dependsOnStageNames =
        dependsOnStages.stream().map(s -> s.getStageName()).collect(Collectors.toList());
    assertThat(expectedDependsOnStageNames)
        .containsExactlyInAnyOrderElementsOf(dependsOnStageNames);
  }

  private void assertGetPermanentFailedStages(
      Set<Stage> stages, List<String> expectedPermanentlyFailedStageNames) {
    List<Stage> permanentlyFailedStages = DependencyResolver.getPermanentlyFailedStages(stages);
    List<String> permanentlyFailedStageNames =
        permanentlyFailedStages.stream().map(s -> s.getStageName()).collect(Collectors.toList());
    assertThat(expectedPermanentlyFailedStageNames)
        .containsExactlyInAnyOrderElementsOf(permanentlyFailedStageNames);
  }

  private void assertGetImmediatelyExecutableStages(
      Process process, List<String> expectedImmediatelyExecutableStageNames) {
    List<Stage> immediatelyExecutableStages =
        DependencyResolver.getImmediatelyExecutableStages(process, Collections.emptySet());
    List<String> immediatelyExecutableStageNames =
        immediatelyExecutableStages.stream()
            .map(s -> s.getStageName())
            .collect(Collectors.toList());
    assertThat(expectedImmediatelyExecutableStageNames)
        .containsExactlyInAnyOrderElementsOf(immediatelyExecutableStageNames);
  }

  private void assertGetEventuallyExecutableStages(
      Process process, List<String> expectedEventuallyExecutableStageNames) {
    List<Stage> eventuallyExecutableStages =
        DependencyResolver.getEventuallyExecutableStages(process);
    List<String> eventuallyExecutableStageNames =
        eventuallyExecutableStages.stream().map(s -> s.getStageName()).collect(Collectors.toList());
    assertThat(expectedEventuallyExecutableStageNames)
        .containsExactlyInAnyOrderElementsOf(eventuallyExecutableStageNames);
  }

  private static void simulateStageExecution(Stage stage, StageState stageState) {
    StageEntity stageEntity = new StageEntity();
    stage.setStageEntity(stageEntity);
    stageEntity.setStageState(PENDING);
    if (stageState == PENDING) {
      return;
    }
    stageEntity.startExecution();
    if (stageState == ACTIVE) {
      return;
    }
    stage.incrementImmediateExecutionCount();
    if (stageState == SUCCESS) {
      stageEntity.endExecution(StageExecutorResult.success());
    } else if (stageState == ERROR) {
      stageEntity.endExecution(StageExecutorResult.error());
    }
  }

  private static void simulateStageExecution(Stage stage, ErrorType errorType) {
    StageEntity stageEntity = new StageEntity();
    stage.setStageEntity(stageEntity);
    stageEntity.setStageState(ERROR);
    stageEntity.startExecution();
    stage.incrementImmediateExecutionCount();
    stageEntity.endExecution(StageExecutorResult.error().setErrorType(errorType));
  }
}
