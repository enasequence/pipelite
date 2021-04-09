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
package pipelite.launcher;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

import org.junit.jupiter.api.Test;
import pipelite.UniqueStringGenerator;
import pipelite.entity.ProcessEntity;
import pipelite.entity.StageEntity;
import pipelite.executor.CallExecutor;
import pipelite.process.Process;
import pipelite.process.builder.ProcessBuilder;
import pipelite.service.StageService;
import pipelite.stage.Stage;
import pipelite.stage.StageState;
import pipelite.stage.executor.StageExecutorResult;
import pipelite.stage.parameters.ExecutorParameters;

public class StageLauncherTest {

  private void callExecutor(StageState stageState) {
    StageService stageService = mock(StageService.class);
    String pipelineName = UniqueStringGenerator.randomPipelineName(StageLauncherTest.class);
    String processId = UniqueStringGenerator.randomProcessId(StageLauncherTest.class);
    Process process =
        new ProcessBuilder(processId)
            .execute("STAGE1")
            .withCallExecutor(
                stageState,
                ExecutorParameters.builder()
                    .immediateRetries(3)
                    .maximumRetries(3)
                    .timeout(null)
                    .build())
            .build();
    process.setProcessEntity(
        ProcessEntity.createExecution(pipelineName, processId, ProcessEntity.DEFAULT_PRIORITY));
    process.getProcessEntity().startExecution();
    Stage stage = process.getStages().get(0);
    stage.setStageEntity(StageEntity.createExecution(pipelineName, processId, stage));
    stage.getStageEntity().startExecution(stage);
    StageLauncher stageLauncher =
        spy(new StageLauncher(stageService, pipelineName, process, stage));
    assertThat(stageLauncher.run().getStageState()).isEqualTo(stageState);
    verify(stageLauncher, times(0)).pollExecution();
    assertThat(stage.getStageEntity().getExecutorName())
        .isEqualTo("pipelite.executor.CallExecutor");
    assertThat(stage.getStageEntity().getExecutorData()).isNull();
    assertThat(stage.getStageEntity().getExecutorParams())
        .isEqualTo("{\n" + "  \"maximumRetries\" : 3,\n" + "  \"immediateRetries\" : 3\n" + "}");
  }

  private void asyncCallExecutor(StageState stageState) {
    StageService stageService = mock(StageService.class);
    String pipelineName = UniqueStringGenerator.randomPipelineName(StageLauncherTest.class);
    String processId = UniqueStringGenerator.randomProcessId(StageLauncherTest.class);
    Process process =
        new ProcessBuilder(processId)
            .execute("STAGE1")
            .withAsyncCallExecutor(
                stageState,
                ExecutorParameters.builder()
                    .immediateRetries(3)
                    .maximumRetries(3)
                    .timeout(null)
                    .build())
            .build();
    process.setProcessEntity(
        ProcessEntity.createExecution(pipelineName, processId, ProcessEntity.DEFAULT_PRIORITY));
    process.getProcessEntity().startExecution();
    Stage stage = process.getStages().get(0);
    stage.setStageEntity(StageEntity.createExecution(pipelineName, processId, stage));
    stage.getStageEntity().startExecution(stage);
    stage.getStageEntity().endExecution(StageExecutorResult.error());
    stage.getStageEntity().startExecution(stage);
    StageLauncher stageLauncher =
        spy(new StageLauncher(stageService, pipelineName, process, stage));
    assertThat(stageLauncher.run().getStageState()).isEqualTo(stageState);
    verify(stageLauncher, times(1)).pollExecution();
    assertThat(stage.getStageEntity().getExecutorName())
        .isEqualTo("pipelite.executor.CallExecutor");
    assertThat(stage.getStageEntity().getExecutorData()).isNull();
    assertThat(stage.getStageEntity().getExecutorParams())
        .isEqualTo("{\n" + "  \"maximumRetries\" : 3,\n" + "  \"immediateRetries\" : 3\n" + "}");
  }

  @Test
  public void callExecutor() {
    callExecutor(StageState.SUCCESS);
    callExecutor(StageState.ERROR);
    asyncCallExecutor(StageState.SUCCESS);
    asyncCallExecutor(StageState.ERROR);
  }

  private void maximumRetries(Integer maximumRetries, int expectedMaximumRetries) {
    CallExecutor executor = new CallExecutor(StageState.SUCCESS);
    executor.setExecutorParams(ExecutorParameters.builder().maximumRetries(maximumRetries).build());
    assertThat(
            StageLauncher.getMaximumRetries(
                Stage.builder().stageName("STAGE").executor(executor).build()))
        .isEqualTo(expectedMaximumRetries);
  }

  private void immediateRetries(
      Integer immediateRetries, Integer maximumRetries, int expectedImmediateRetries) {
    CallExecutor executor = new CallExecutor(StageState.SUCCESS);
    executor.setExecutorParams(
        ExecutorParameters.builder()
            .immediateRetries(immediateRetries)
            .maximumRetries(maximumRetries)
            .build());
    assertThat(
            StageLauncher.getImmediateRetries(
                Stage.builder().stageName("STAGE").executor(executor).build()))
        .isEqualTo(expectedImmediateRetries);
  }

  @Test
  public void maximumRetries() {
    maximumRetries(1, 1);
    maximumRetries(5, 5);
    maximumRetries(null, ExecutorParameters.DEFAULT_MAX_RETRIES);
  }

  @Test
  public void immediateRetries() {
    immediateRetries(3, 6, 3);
    immediateRetries(3, 2, 2);
    immediateRetries(3, 0, 0);
    immediateRetries(
        null,
        ExecutorParameters.DEFAULT_IMMEDIATE_RETRIES + 1,
        ExecutorParameters.DEFAULT_IMMEDIATE_RETRIES);
    immediateRetries(null, null, ExecutorParameters.DEFAULT_IMMEDIATE_RETRIES);
  }
}
