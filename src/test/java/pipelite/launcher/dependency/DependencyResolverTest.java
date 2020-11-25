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
import static pipelite.stage.StageExecutionResultType.ERROR;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;
import pipelite.UniqueStringGenerator;
import pipelite.entity.StageEntity;
import pipelite.executor.SuccessSyncExecutor;
import pipelite.process.Process;
import pipelite.process.builder.ProcessBuilder;
import pipelite.stage.Stage;
import pipelite.stage.StageExecutionResult;
import pipelite.stage.StageExecutionResultType;
import pipelite.executor.StageExecutorParameters;

public class DependencyResolverTest {

  @Test
  public void testGetRunnableStageAllActiveAllDependOnPrevious() {
    List<Stage> stages = new ArrayList<>();

    ProcessBuilder builder = new ProcessBuilder(UniqueStringGenerator.randomProcessId());
    Process process =
        builder
            .execute("STAGE1")
            .with(new SuccessSyncExecutor())
            .executeAfterPrevious("STAGE2")
            .with(new SuccessSyncExecutor())
            .executeAfterPrevious("STAGE3")
            .with(new SuccessSyncExecutor())
            .build();

    for (Stage stage : process.getStages()) {
      StageEntity stageEntity = new StageEntity();
      stage.setStageEntity(stageEntity);
      stageEntity.startExecution(stage);
      stages.add(stage);
    }

    List<Stage> executableStages = DependencyResolver.getExecutableStages(stages);

    assertThat(executableStages.size()).isOne();
    assertThat(executableStages.get(0).getStageName()).isEqualTo("STAGE1");
  }

  @Test
  public void testGetRunnableStageAllActiveAllDependOnFirst() {
    List<Stage> stages = new ArrayList<>();

    ProcessBuilder builder = new ProcessBuilder(UniqueStringGenerator.randomProcessId());
    Process process =
        builder
            .execute("STAGE1")
            .with(new SuccessSyncExecutor())
            .executeAfter("STAGE2", "STAGE1")
            .with(new SuccessSyncExecutor())
            .executeAfter("STAGE3", "STAGE1")
            .with(new SuccessSyncExecutor())
            .build();

    for (Stage stage : process.getStages()) {
      StageEntity stageEntity = new StageEntity();
      stage.setStageEntity(stageEntity);
      stageEntity.startExecution(stage);
      stages.add(stage);
    }

    List<Stage> executableStages = DependencyResolver.getExecutableStages(stages);

    assertThat(executableStages.size()).isOne();
    assertThat(executableStages.get(0).getStageName()).isEqualTo("STAGE1");
  }

  @Test
  public void testGetRunnableStageFirstSuccessAllDependOnFirst() {
    List<Stage> stages = new ArrayList<>();

    ProcessBuilder builder = new ProcessBuilder(UniqueStringGenerator.randomProcessId());
    Process process =
        builder
            .execute("STAGE1")
            .with(new SuccessSyncExecutor())
            .executeAfter("STAGE2", "STAGE1")
            .with(new SuccessSyncExecutor())
            .executeAfter("STAGE3", "STAGE1")
            .with(new SuccessSyncExecutor())
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

    List<Stage> executableStages = DependencyResolver.getExecutableStages(stages);

    assertThat(executableStages.size()).isEqualTo(2);
    assertThat(executableStages.get(0).getStageName()).isEqualTo("STAGE2");
    assertThat(executableStages.get(1).getStageName()).isEqualTo("STAGE3");
  }

  @Test
  public void testGetRunnableStageFirstErrorMaxRetriesAllDependOnFirst() {
    List<Stage> stages = new ArrayList<>();

    ProcessBuilder builder = new ProcessBuilder(UniqueStringGenerator.randomProcessId());
    Process process =
        builder
            .execute("STAGE1", StageExecutorParameters.builder().maximumRetries(0).build())
            .with(new SuccessSyncExecutor())
            .executeAfter("STAGE2", "STAGE1")
            .with(new SuccessSyncExecutor())
            .executeAfter("STAGE3", "STAGE1")
            .with(new SuccessSyncExecutor())
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

    List<Stage> executableStages = DependencyResolver.getExecutableStages(stages);

    assertThat(executableStages.size()).isEqualTo(0);
  }

  @Test
  public void testGetRunnableStageFirstErrorMaxImmediateAllDependOnFirst() {
    List<Stage> stages = new ArrayList<>();

    ProcessBuilder builder = new ProcessBuilder(UniqueStringGenerator.randomProcessId());
    Process process =
        builder
            .execute(
                "STAGE1", StageExecutorParameters.builder().maximumRetries(3).immediateRetries(0).build())
            .with(new SuccessSyncExecutor())
            .executeAfter("STAGE2", "STAGE1")
            .with(new SuccessSyncExecutor())
            .executeAfter("STAGE3", "STAGE1")
            .with(new SuccessSyncExecutor())
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

    List<Stage> executableStages = DependencyResolver.getExecutableStages(stages);

    assertThat(executableStages.size()).isEqualTo(0);
  }

  @Test
  public void testGetRunnableStageFirstErrorNotMaxRetriesImmediateAllDependOnFirst() {
    List<Stage> stages = new ArrayList<>();

    ProcessBuilder builder = new ProcessBuilder(UniqueStringGenerator.randomProcessId());
    Process process =
        builder
            .execute(
                "STAGE1", StageExecutorParameters.builder().maximumRetries(1).immediateRetries(1).build())
            .with(new SuccessSyncExecutor())
            .executeAfter("STAGE2", "STAGE1")
            .with(new SuccessSyncExecutor())
            .executeAfter("STAGE3", "STAGE1")
            .with(new SuccessSyncExecutor())
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

    List<Stage> executableStages = DependencyResolver.getExecutableStages(stages);

    assertThat(executableStages.size()).isOne();
    assertThat(executableStages.get(0).getStageName()).isEqualTo("STAGE1");
  }

  @Test
  public void testGetDependentStagesAllDependOnPrevious() {
    List<Stage> stages = new ArrayList<>();

    ProcessBuilder builder = new ProcessBuilder(UniqueStringGenerator.randomProcessId());
    Process process =
        builder
            .execute("STAGE1")
            .with(new SuccessSyncExecutor())
            .executeAfterPrevious("STAGE2")
            .with(new SuccessSyncExecutor())
            .executeAfterPrevious("STAGE3")
            .with(new SuccessSyncExecutor())
            .executeAfterPrevious("STAGE4")
            .with(new SuccessSyncExecutor())
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
  public void testGetDependentStagesAllDependOnFirst() {
    List<Stage> stages = new ArrayList<>();

    ProcessBuilder builder = new ProcessBuilder(UniqueStringGenerator.randomProcessId());
    Process process =
        builder
            .execute("STAGE1")
            .with(new SuccessSyncExecutor())
            .executeAfter("STAGE2", "STAGE1")
            .with(new SuccessSyncExecutor())
            .executeAfter("STAGE3", "STAGE1")
            .with(new SuccessSyncExecutor())
            .executeAfter("STAGE4", "STAGE1")
            .with(new SuccessSyncExecutor())
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

  private void getDependsOnStages(List<Stage> stages, int stageIndex, int dependsOnSize) {
    assertThat(DependencyResolver.getDependsOnStages(stages, stages.get(stageIndex)).size())
        .isEqualTo(dependsOnSize);
  }

  private void getDependsOnStages(
      List<Stage> stages, int stageIndex, int dependsOnIndex, String dependsOnStageName) {
    assertThat(
            DependencyResolver.getDependsOnStages(stages, stages.get(stageIndex))
                .get(dependsOnIndex)
                .getStageName())
        .isEqualTo(dependsOnStageName);
  }

  @Test
  public void getDependsOnStages() {
    ProcessBuilder builder = new ProcessBuilder(UniqueStringGenerator.randomProcessId());
    Process process =
        builder
            .execute("STAGE1")
            .with(new SuccessSyncExecutor())
            .executeAfter("STAGE2", "STAGE1")
            .with(new SuccessSyncExecutor())
            .executeAfter("STAGE3", Arrays.asList("STAGE1", "STAGE2"))
            .with(new SuccessSyncExecutor())
            .executeAfter("STAGE4", Arrays.asList("STAGE3", "STAGE3"))
            .with(new SuccessSyncExecutor())
            .build();

    List<Stage> stages = new ArrayList<>();
    for (Stage stage : process.getStages()) {
      StageEntity stageEntity = new StageEntity();
      stage.setStageEntity(stageEntity);
      stages.add(stage);
    }

    getDependsOnStages(stages, 0, 0);
    getDependsOnStages(stages, 1, 1);
    getDependsOnStages(stages, 2, 2);
    getDependsOnStages(stages, 3, 3);
    getDependsOnStages(stages, 1, 0, "STAGE1");
    getDependsOnStages(stages, 2, 0, "STAGE1");
    getDependsOnStages(stages, 2, 1, "STAGE2");
    getDependsOnStages(stages, 3, 0, "STAGE3");
    getDependsOnStages(stages, 3, 1, "STAGE1");
    getDependsOnStages(stages, 3, 2, "STAGE2");
  }

  public static List<Stage> isDependsOnStageSuccessStages(
      StageExecutionResultType dependsOnResultType, StageExecutionResultType secondResultType) {
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
    firstStageEntity.setResultType(dependsOnResultType);
    stages.add(firstStage);

    Stage secondStage = process.getStages().get(1);
    StageEntity secondStageEntity = new StageEntity();
    secondStage.setStageEntity(secondStageEntity);
    secondStageEntity.setResultType(dependsOnResultType);
    stages.add(secondStage);

    Stage lastStage = process.getStages().get(2);
    StageEntity lastStageEntity = new StageEntity();
    lastStage.setStageEntity(lastStageEntity);
    lastStageEntity.setResultType(secondResultType);
    stages.add(lastStage);

    return stages;
  }

  private void isDependsOnStageSuccess(
      StageExecutionResultType dependsOnResultType, boolean isDependsOnStageCompleted) {
    List<Stage> stages = isDependsOnStageSuccessStages(dependsOnResultType, NEW);
    assertThat(DependencyResolver.isDependsOnStagesSuccess(stages, stages.get(2)))
        .isEqualTo(isDependsOnStageCompleted);
  }

  @Test
  public void isDependsOnStageSuccess() {
    isDependsOnStageSuccess(NEW, false);
    isDependsOnStageSuccess(SUCCESS, true);
    isDependsOnStageSuccess(ACTIVE, false);
    isDependsOnStageSuccess(ERROR, false);
    isDependsOnStageSuccess(null, false);
  }
}
