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

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import pipelite.UniqueStringGenerator;
import pipelite.entity.StageEntity;
import pipelite.executor.SuccessSyncExecutor;
import pipelite.launcher.ProcessLauncher;
import pipelite.process.Process;
import pipelite.process.builder.ProcessBuilder;
import pipelite.stage.Stage;
import pipelite.stage.StageExecutionResult;
import pipelite.stage.StageParameters;

public class DependencyResolverTest {

  @Test
  public void testGetRunnableStageAllActiveAllDependOnPrevious() {
    List<ProcessLauncher.StageExecution> stageAndStageEntities = new ArrayList<>();

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
      stageEntity.startExecution(stage);
      stageAndStageEntities.add(new ProcessLauncher.StageExecution(stage, stageEntity));
    }

    DependencyResolver dependencyResolver = new DependencyResolver(stageAndStageEntities);

    List<ProcessLauncher.StageExecution> executableStages =
        dependencyResolver.getExecutableStages();

    assertThat(executableStages.size()).isOne();
    assertThat(executableStages.get(0).getStage().getStageName()).isEqualTo("STAGE1");
  }

  @Test
  public void testGetRunnableStageAllActiveAllDependOnFirst() {
    List<ProcessLauncher.StageExecution> stageAndStageEntities = new ArrayList<>();

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
      stageEntity.startExecution(stage);
      stageAndStageEntities.add(new ProcessLauncher.StageExecution(stage, stageEntity));
    }

    DependencyResolver dependencyResolver = new DependencyResolver(stageAndStageEntities);

    List<ProcessLauncher.StageExecution> executableStages =
        dependencyResolver.getExecutableStages();

    assertThat(executableStages.size()).isOne();
    assertThat(executableStages.get(0).getStage().getStageName()).isEqualTo("STAGE1");
  }

  @Test
  public void testGetRunnableStageFirstSuccessAllDependOnFirst() {
    List<ProcessLauncher.StageExecution> stageAndStageEntities = new ArrayList<>();

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
      if (stageNumber == 0) {
        stageEntity.startExecution(stage);
        stageEntity.endExecution(StageExecutionResult.success());
      }
      stageNumber++;
      stageAndStageEntities.add(new ProcessLauncher.StageExecution(stage, stageEntity));
    }

    DependencyResolver dependencyResolver = new DependencyResolver(stageAndStageEntities);

    List<ProcessLauncher.StageExecution> executableStages =
        dependencyResolver.getExecutableStages();

    assertThat(executableStages.size()).isEqualTo(2);
    assertThat(executableStages.get(0).getStage().getStageName()).isEqualTo("STAGE2");
    assertThat(executableStages.get(1).getStage().getStageName()).isEqualTo("STAGE3");
  }

  @Test
  public void testGetRunnableStageFirstErrorMaxRetriesAllDependOnFirst() {
    List<ProcessLauncher.StageExecution> stageAndStageEntities = new ArrayList<>();

    ProcessBuilder builder = new ProcessBuilder(UniqueStringGenerator.randomProcessId());
    Process process =
        builder
            .execute(
                "STAGE1", StageParameters.builder().maximumRetries(0).build())
            .with(new SuccessSyncExecutor())
            .executeAfter("STAGE2", "STAGE1")
            .with(new SuccessSyncExecutor())
            .executeAfter("STAGE3", "STAGE1")
            .with(new SuccessSyncExecutor())
            .build();

    int stageNumber = 0;
    for (Stage stage : process.getStages()) {
      StageEntity stageEntity = new StageEntity();
      if (stageNumber == 0) {
        stageEntity.startExecution(stage);
        stageEntity.endExecution(StageExecutionResult.error());
        ProcessLauncher.StageExecution stageExecution =
            new ProcessLauncher.StageExecution(stage, stageEntity);
        stageExecution.incrementImmediateExecutionCount();
        stageAndStageEntities.add(stageExecution);
      } else {
        stageAndStageEntities.add(new ProcessLauncher.StageExecution(stage, stageEntity));
      }
      stageNumber++;
    }

    DependencyResolver dependencyResolver = new DependencyResolver(stageAndStageEntities);

    List<ProcessLauncher.StageExecution> executableStages =
        dependencyResolver.getExecutableStages();

    assertThat(executableStages.size()).isEqualTo(0);
  }

  @Test
  public void testGetRunnableStageFirstErrorMaxImmediateAllDependOnFirst() {
    List<ProcessLauncher.StageExecution> stageAndStageEntities = new ArrayList<>();

    ProcessBuilder builder = new ProcessBuilder(UniqueStringGenerator.randomProcessId());
    Process process =
        builder
            .execute(
                "STAGE1", StageParameters.builder().maximumRetries(3).immediateRetries(0).build())
            .with(new SuccessSyncExecutor())
            .executeAfter("STAGE2", "STAGE1")
            .with(new SuccessSyncExecutor())
            .executeAfter("STAGE3", "STAGE1")
            .with(new SuccessSyncExecutor())
            .build();

    int stageNumber = 0;
    for (Stage stage : process.getStages()) {
      StageEntity stageEntity = new StageEntity();
      if (stageNumber == 0) {
        stageEntity.startExecution(stage);
        stageEntity.endExecution(StageExecutionResult.error());
        ProcessLauncher.StageExecution stageExecution =
            new ProcessLauncher.StageExecution(stage, stageEntity);
        stageExecution.incrementImmediateExecutionCount();
        stageAndStageEntities.add(stageExecution);
      } else {
        stageAndStageEntities.add(new ProcessLauncher.StageExecution(stage, stageEntity));
      }
      stageNumber++;
    }

    DependencyResolver dependencyResolver = new DependencyResolver(stageAndStageEntities);

    List<ProcessLauncher.StageExecution> executableStages =
        dependencyResolver.getExecutableStages();

    assertThat(executableStages.size()).isEqualTo(0);
  }

  @Test
  public void testGetRunnableStageFirstErrorNotMaxRetriesImmediateAllDependOnFirst() {
    List<ProcessLauncher.StageExecution> stageAndStageEntities = new ArrayList<>();

    ProcessBuilder builder = new ProcessBuilder(UniqueStringGenerator.randomProcessId());
    Process process =
        builder
            .execute(
                "STAGE1", StageParameters.builder().maximumRetries(1).immediateRetries(1).build())
            .with(new SuccessSyncExecutor())
            .executeAfter("STAGE2", "STAGE1")
            .with(new SuccessSyncExecutor())
            .executeAfter("STAGE3", "STAGE1")
            .with(new SuccessSyncExecutor())
            .build();

    int stageNumber = 0;
    for (Stage stage : process.getStages()) {
      StageEntity stageEntity = new StageEntity();
      if (stageNumber == 0) {
        stageEntity.startExecution(stage);
        stageEntity.endExecution(StageExecutionResult.error());
        ProcessLauncher.StageExecution stageExecution =
            new ProcessLauncher.StageExecution(stage, stageEntity);
        stageExecution.incrementImmediateExecutionCount();
        stageAndStageEntities.add(stageExecution);
      } else {
        stageAndStageEntities.add(new ProcessLauncher.StageExecution(stage, stageEntity));
      }
      stageNumber++;
    }

    DependencyResolver dependencyResolver = new DependencyResolver(stageAndStageEntities);

    List<ProcessLauncher.StageExecution> executableStages =
        dependencyResolver.getExecutableStages();

    assertThat(executableStages.size()).isOne();
    assertThat(executableStages.get(0).getStage().getStageName()).isEqualTo("STAGE1");
  }

  @Test
  public void testGetDependentStagesAllDependOnPrevious() {
    List<ProcessLauncher.StageExecution> stageAndStageEntities = new ArrayList<>();

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
      stageAndStageEntities.add(new ProcessLauncher.StageExecution(stage, stageEntity));
    }

    DependencyResolver dependencyResolver = new DependencyResolver(stageAndStageEntities);

    List<ProcessLauncher.StageExecution> dependentStages =
        dependencyResolver.getDependentStages(stageAndStageEntities.get(0));
    assertThat(dependentStages.size()).isEqualTo(3);
    assertThat(dependentStages.get(0).getStage().getStageName()).isEqualTo("STAGE4");
    assertThat(dependentStages.get(1).getStage().getStageName()).isEqualTo("STAGE3");
    assertThat(dependentStages.get(2).getStage().getStageName()).isEqualTo("STAGE2");

    dependentStages = dependencyResolver.getDependentStages(stageAndStageEntities.get(1));
    assertThat(dependentStages.size()).isEqualTo(2);
    assertThat(dependentStages.get(0).getStage().getStageName()).isEqualTo("STAGE4");
    assertThat(dependentStages.get(1).getStage().getStageName()).isEqualTo("STAGE3");

    dependentStages = dependencyResolver.getDependentStages(stageAndStageEntities.get(2));
    assertThat(dependentStages.size()).isEqualTo(1);
    assertThat(dependentStages.get(0).getStage().getStageName()).isEqualTo("STAGE4");
  }

  @Test
  public void testGetDependentStagesAllDependOnFirst() {
    List<ProcessLauncher.StageExecution> stageAndStageEntities = new ArrayList<>();

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
      stageAndStageEntities.add(new ProcessLauncher.StageExecution(stage, stageEntity));
    }

    DependencyResolver dependencyResolver = new DependencyResolver(stageAndStageEntities);

    List<ProcessLauncher.StageExecution> dependentStages =
        dependencyResolver.getDependentStages(stageAndStageEntities.get(0));
    assertThat(dependentStages.size()).isEqualTo(3);
    assertThat(dependentStages.get(0).getStage().getStageName()).isEqualTo("STAGE2");
    assertThat(dependentStages.get(1).getStage().getStageName()).isEqualTo("STAGE3");
    assertThat(dependentStages.get(2).getStage().getStageName()).isEqualTo("STAGE4");

    dependentStages = dependencyResolver.getDependentStages(stageAndStageEntities.get(1));
    assertThat(dependentStages.size()).isEqualTo(0);

    dependentStages = dependencyResolver.getDependentStages(stageAndStageEntities.get(2));
    assertThat(dependentStages.size()).isEqualTo(0);

    dependentStages = dependencyResolver.getDependentStages(stageAndStageEntities.get(3));
    assertThat(dependentStages.size()).isEqualTo(0);
  }
}
