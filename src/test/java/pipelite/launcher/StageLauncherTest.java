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
import pipelite.configuration.LauncherConfiguration;
import pipelite.configuration.StageConfiguration;
import pipelite.entity.ProcessEntity;
import pipelite.entity.StageEntity;
import pipelite.executor.StageExecutorParameters;
import pipelite.process.Process;
import pipelite.process.builder.ProcessBuilder;
import pipelite.stage.Stage;
import pipelite.stage.StageExecutionResult;

public class StageLauncherTest {

  /*
    public class ResultCapture<T> implements Answer {
      private T result = null;

      public T getResult() {
        return result;
      }

      @Override
      public T answer(InvocationOnMock invocationOnMock) throws Throwable {
        result = (T) invocationOnMock.callRealMethod();
        return result;
      }
    }
  */

  private void testRunSyncExecutor(StageExecutionResult result) {
    LauncherConfiguration launcherConfiguration = new LauncherConfiguration();
    StageConfiguration stageConfiguration = new StageConfiguration();
    String pipelineName = UniqueStringGenerator.randomPipelineName();
    String processId = UniqueStringGenerator.randomProcessId();
    Process process =
        new ProcessBuilder(processId)
            .execute(
                "STAGE1",
                StageExecutorParameters.builder().immediateRetries(3).maximumRetries(3).build())
            .withEmptySyncExecutor(result.getResultType())
            .build();
    process.setProcessEntity(
        ProcessEntity.createExecution(pipelineName, processId, ProcessEntity.DEFAULT_PRIORITY));
    process.getProcessEntity().startExecution();
    Stage stage = process.getStages().get(0);
    stage.setStageEntity(StageEntity.createExecution(pipelineName, processId, stage));
    stage.getStageEntity().startExecution(stage);
    StageLauncher stageLauncher =
        spy(
            new StageLauncher(
                launcherConfiguration, stageConfiguration, pipelineName, process, stage));
    assertThat(stageLauncher.run()).isEqualTo(result);
    verify(stageLauncher, times(0)).pollExecution();
    assertThat(stage.getStageEntity().getExecutorName())
        .isEqualTo("pipelite.executor.EmptySyncStageExecutor");
    assertThat(stage.getStageEntity().getExecutorData())
        .isEqualTo("{\n" + "  \"resultType\" : \"" + result.getResultType().name() + "\"\n}");
    assertThat(stage.getStageEntity().getExecutorParams())
        .isEqualTo("{\n" + "  \"maximumRetries\" : 3,\n" + "  \"immediateRetries\" : 3\n" + "}");
  }

  private void testRunAsyncExecutor(StageExecutionResult result) {
    LauncherConfiguration launcherConfiguration = new LauncherConfiguration();
    StageConfiguration stageConfiguration = new StageConfiguration();
    String pipelineName = UniqueStringGenerator.randomPipelineName();
    String processId = UniqueStringGenerator.randomProcessId();
    Process process =
        new ProcessBuilder(processId)
            .execute(
                "STAGE1",
                StageExecutorParameters.builder().immediateRetries(3).maximumRetries(3).build())
            .withEmptyAsyncExecutor(result.getResultType())
            .build();
    process.setProcessEntity(
        ProcessEntity.createExecution(pipelineName, processId, ProcessEntity.DEFAULT_PRIORITY));
    process.getProcessEntity().startExecution();
    Stage stage = process.getStages().get(0);
    stage.setStageEntity(StageEntity.createExecution(pipelineName, processId, stage));
    stage.getStageEntity().startExecution(stage);
    stage.getStageEntity().endExecution(StageExecutionResult.error());
    stage.getStageEntity().startExecution(stage);
    StageLauncher stageLauncher =
        spy(
            new StageLauncher(
                launcherConfiguration, stageConfiguration, pipelineName, process, stage));
    assertThat(stageLauncher.run()).isEqualTo(result);
    verify(stageLauncher, times(1)).pollExecution();
    assertThat(stage.getStageEntity().getExecutorName())
        .isEqualTo("pipelite.executor.EmptyAsyncStageExecutor");
    assertThat(stage.getStageEntity().getExecutorData())
        .isEqualTo("{\n" + "  \"resultType\" : \"" + result.getResultType().name() + "\"\n}");
    assertThat(stage.getStageEntity().getExecutorParams())
        .isEqualTo("{\n" + "  \"maximumRetries\" : 3,\n" + "  \"immediateRetries\" : 3\n" + "}");
  }

  @Test
  public void run() {
    testRunSyncExecutor(StageExecutionResult.success());
    testRunSyncExecutor(StageExecutionResult.error());
    testRunAsyncExecutor(StageExecutionResult.success());
    testRunAsyncExecutor(StageExecutionResult.error());
  }

  private void testMaximumRetries(Integer maximumRetries, int expectedMaximumRetries) {
    assertThat(
            StageLauncher.getMaximumRetries(
                Stage.builder()
                    .stageName("STAGE")
                    .executor((pipelineName, processId, stage) -> StageExecutionResult.success())
                    .executorParams(
                        StageExecutorParameters.builder().maximumRetries(maximumRetries).build())
                    .build()))
        .isEqualTo(expectedMaximumRetries);
  }

  private void testImmediateRetries(
      Integer immediateRetries, Integer maximumRetries, int expectedImmediateRetries) {
    assertThat(
            StageLauncher.getImmediateRetries(
                Stage.builder()
                    .stageName("STAGE")
                    .executor((pipelineName, processId, stage) -> StageExecutionResult.success())
                    .executorParams(
                        StageExecutorParameters.builder()
                            .maximumRetries(maximumRetries)
                            .immediateRetries(immediateRetries)
                            .build())
                    .build()))
        .isEqualTo(expectedImmediateRetries);
  }

  @Test
  public void maximumRetries() {
    testMaximumRetries(1, 1);
    testMaximumRetries(5, 5);
    testMaximumRetries(null, StageConfiguration.DEFAULT_MAX_RETRIES);
  }

  @Test
  public void immediateRetries() {
    testImmediateRetries(3, 6, 3);
    testImmediateRetries(3, 2, 2);
    testImmediateRetries(3, 0, 0);
    testImmediateRetries(
        null,
        StageConfiguration.DEFAULT_IMMEDIATE_RETRIES + 1,
        StageConfiguration.DEFAULT_IMMEDIATE_RETRIES);
    testImmediateRetries(null, null, StageConfiguration.DEFAULT_IMMEDIATE_RETRIES);
  }
}
