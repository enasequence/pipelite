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
import static org.mockito.Mockito.spy;

import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import pipelite.PipeliteTestConfigWithManager;
import pipelite.UniqueStringGenerator;
import pipelite.entity.ProcessEntity;
import pipelite.entity.StageEntity;
import pipelite.executor.TestExecutor;
import pipelite.metrics.PipeliteMetrics;
import pipelite.process.Process;
import pipelite.process.builder.ProcessBuilder;
import pipelite.service.PipeliteServices;
import pipelite.stage.Stage;
import pipelite.stage.StageState;
import pipelite.stage.executor.StageExecutorResult;
import pipelite.stage.parameters.CmdExecutorParameters;
import pipelite.stage.parameters.ExecutorParameters;

@SpringBootTest(
    classes = PipeliteTestConfigWithManager.class,
    properties = {"pipelite.service.force=true", "pipelite.service.name=StageRunnerTest"})
@ActiveProfiles({"test"})
@DirtiesContext
public class StageRunnerTest {

  @Autowired PipeliteServices pipeliteServices;
  @Autowired PipeliteMetrics pipeliteMetrics;

  private void sync(StageState stageState) {
    String serviceName = UniqueStringGenerator.randomServiceName(StageRunnerTest.class);
    String pipelineName = UniqueStringGenerator.randomPipelineName(StageRunnerTest.class);
    String processId = UniqueStringGenerator.randomProcessId(StageRunnerTest.class);
    Process process =
        new ProcessBuilder(processId)
            .execute("STAGE1")
            .withSyncTestExecutor(
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
    StageRunner stageRunner =
        spy(
            new StageRunner(
                pipeliteServices, pipeliteMetrics, serviceName, pipelineName, process, stage));

    AtomicReference<StageExecutorResult> result = new AtomicReference<>();
    stageRunner.runOneIteration(r -> result.set(r));

    assertThat(result.get()).isNotNull();
    assertThat(result.get().getStageState()).isEqualTo(stageState);

    assertThat(stage.getStageEntity().getExecutorName())
        .isEqualTo("pipelite.executor.TestExecutor");
    assertThat(stage.getStageEntity().getExecutorData()).isNull();
    assertThat(stage.getStageEntity().getExecutorParams())
        .isEqualTo("{\n" + "  \"maximumRetries\" : 3,\n" + "  \"immediateRetries\" : 3\n" + "}");
  }

  private void async(StageState stageState) {
    String serviceName = UniqueStringGenerator.randomServiceName(StageRunnerTest.class);
    String pipelineName = UniqueStringGenerator.randomPipelineName(StageRunnerTest.class);
    String processId = UniqueStringGenerator.randomProcessId(StageRunnerTest.class);
    Process process =
        new ProcessBuilder(processId)
            .execute("STAGE1")
            .withAsyncTestExecutor(
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
    StageRunner stageRunner =
        spy(
            new StageRunner(
                pipeliteServices, pipeliteMetrics, serviceName, pipelineName, process, stage));

    AtomicReference<StageExecutorResult> result = new AtomicReference<>();

    stageRunner.runOneIteration(r -> result.set(r));

    assertThat(result.get()).isNull();
    assertThat(stage.getStageEntity().getStageState()).isEqualTo(StageState.ACTIVE);

    stageRunner.runOneIteration(r -> result.set(r));

    assertThat(result.get()).isNotNull();
    assertThat(result.get().getStageState()).isEqualTo(stageState);

    assertThat(stage.getStageEntity().getExecutorName())
        .isEqualTo("pipelite.executor.TestExecutor");
    assertThat(stage.getStageEntity().getExecutorData()).isNull();
    assertThat(stage.getStageEntity().getExecutorParams())
        .isEqualTo("{\n" + "  \"maximumRetries\" : 3,\n" + "  \"immediateRetries\" : 3\n" + "}");
  }

  @Test
  public void sync() {
    sync(StageState.SUCCESS);
    sync(StageState.ERROR);
  }

  @Test
  public void async() {
    async(StageState.SUCCESS);
    async(StageState.ERROR);
  }

  private void maximumRetries(Integer maximumRetries, int expectedMaximumRetries) {
    TestExecutor executor = TestExecutor.sync(StageState.SUCCESS);
    executor.setExecutorParams(ExecutorParameters.builder().maximumRetries(maximumRetries).build());
    assertThat(
            StageRunner.getMaximumRetries(
                Stage.builder().stageName("STAGE").executor(executor).build()))
        .isEqualTo(expectedMaximumRetries);
  }

  private void immediateRetries(
      Integer immediateRetries, Integer maximumRetries, int expectedImmediateRetries) {
    TestExecutor executor = TestExecutor.sync(StageState.SUCCESS);
    executor.setExecutorParams(
        ExecutorParameters.builder()
            .immediateRetries(immediateRetries)
            .maximumRetries(maximumRetries)
            .build());
    assertThat(
            StageRunner.getImmediateRetries(
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

  @Test
  public void cmdExecutorSuccess() {
    String serviceName = UniqueStringGenerator.randomServiceName(StageRunnerTest.class);
    String pipelineName = UniqueStringGenerator.randomPipelineName(StageRunnerTest.class);
    String processId = UniqueStringGenerator.randomProcessId(StageRunnerTest.class);
    Process process =
        new ProcessBuilder(processId)
            .execute("STAGE1")
            .withCmdExecutor(
                "date",
                CmdExecutorParameters.builder()
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

    assertThat(stage.getStageEntity().getStageState()).isEqualTo(StageState.ACTIVE);
    assertThat(stage.getStageEntity().getExecutionCount()).isEqualTo(0);

    StageRunner stageRunner =
        new StageRunner(
            pipeliteServices, pipeliteMetrics, serviceName, pipelineName, process, stage);

    AtomicReference<StageExecutorResult> result = new AtomicReference<>();
    stageRunner.runOneIteration(r -> result.set(r));

    assertThat(result.get()).isNotNull();
    assertThat(result.get().getStageState()).isEqualTo(StageState.SUCCESS);
    assertThat(result.get().isSuccess()).isTrue();
    assertThat(stage.getStageEntity().getStageState()).isEqualTo(StageState.SUCCESS);
    assertThat(stage.getStageEntity().getExecutionCount()).isEqualTo(1);
  }

  @Test
  public void cmdExecutorPermanentError() {
    String serviceName = UniqueStringGenerator.randomServiceName(StageRunnerTest.class);
    String pipelineName = UniqueStringGenerator.randomPipelineName(StageRunnerTest.class);
    String processId = UniqueStringGenerator.randomProcessId(StageRunnerTest.class);
    Process process =
        new ProcessBuilder(processId)
            .execute("STAGE1")
            .withCmdExecutor(
                "date",
                CmdExecutorParameters.builder()
                    .permanentError(0)
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

    assertThat(stage.getStageEntity().getStageState()).isEqualTo(StageState.ACTIVE);
    assertThat(stage.getStageEntity().getExecutionCount()).isEqualTo(0);

    StageRunner stageRunner =
        new StageRunner(
            pipeliteServices, pipeliteMetrics, serviceName, pipelineName, process, stage);

    AtomicReference<StageExecutorResult> result = new AtomicReference<>();
    stageRunner.runOneIteration(r -> result.set(r));

    assertThat(result.get()).isNotNull();
    assertThat(result.get().getStageState()).isEqualTo(StageState.ERROR);
    assertThat(result.get().isError()).isTrue();
    assertThat(result.get().isPermanentError()).isTrue();
    assertThat(stage.getStageEntity().getStageState()).isEqualTo(StageState.ERROR);
    // Permanent error results in the maximum execution count being exceeded.
    assertThat(stage.getStageEntity().getExecutionCount()).isEqualTo(4);
  }
}
