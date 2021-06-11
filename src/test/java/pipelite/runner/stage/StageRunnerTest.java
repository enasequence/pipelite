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

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.spy;

import java.util.Collections;
import java.util.EnumSet;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import pipelite.PipeliteTestConfigWithManager;
import pipelite.UniqueStringGenerator;
import pipelite.configuration.properties.LsfTestConfiguration;
import pipelite.entity.ProcessEntity;
import pipelite.entity.StageEntity;
import pipelite.helper.CreateProcessSingleStageSimpleLsfPipelineTestHelper;
import pipelite.helper.SimpleLsfExecutorTestHelper;
import pipelite.metrics.PipeliteMetrics;
import pipelite.process.Process;
import pipelite.process.builder.ProcessBuilder;
import pipelite.service.PipeliteServices;
import pipelite.stage.Stage;
import pipelite.stage.StageState;
import pipelite.stage.executor.StageExecutorResult;
import pipelite.stage.executor.StageExecutorState;
import pipelite.stage.parameters.ExecutorParameters;

@SpringBootTest(
    classes = PipeliteTestConfigWithManager.class,
    properties = {"pipelite.service.force=true", "pipelite.service.name=StageRunnerTest"})
@ActiveProfiles({"test"})
@DirtiesContext
public class StageRunnerTest {

  @Autowired PipeliteServices pipeliteServices;
  @Autowired PipeliteMetrics pipeliteMetrics;
  @Autowired LsfTestConfiguration lsfTestConfiguration;

  private static final int RETRY_CNT = 1;

  private interface SingleStageProcessFactory {
    Process create(StageExecutorState executorState, int immediateRetries, int maximumRetries);
  }

  private Process singleStageProcessSyncTestExecutorFactory(
      StageExecutorState executorState, int immediateRetries, int maximumRetries) {
    String processId = UniqueStringGenerator.randomProcessId(StageRunnerTest.class);
    return new ProcessBuilder(processId)
        .execute("STAGE1")
        .withSyncTestExecutor(
            executorState,
            ExecutorParameters.builder()
                .immediateRetries(immediateRetries)
                .maximumRetries(maximumRetries)
                .build())
        .build();
  }

  private Process singleStageProcessAsyncTestExecutorFactory(
      StageExecutorState executorState, int immediateRetries, int maximumRetries) {
    String processId = UniqueStringGenerator.randomProcessId(StageRunnerTest.class);
    return new ProcessBuilder(processId)
        .execute("STAGE1")
        .withAsyncTestExecutor(
            executorState,
            ExecutorParameters.builder()
                .immediateRetries(immediateRetries)
                .maximumRetries(maximumRetries)
                .build())
        .build();
  }

  private static class SimpleLsfPipeline
      extends CreateProcessSingleStageSimpleLsfPipelineTestHelper {
    public SimpleLsfPipeline(
        int exitCode,
        int immediateRetries,
        int maxRetries,
        LsfTestConfiguration lsfTestConfiguration) {
      super(1, exitCode, 1, immediateRetries, maxRetries, lsfTestConfiguration);
    }
  }

  private Process singleStageProcessSimpleLsfExecutorFactory(
      StageExecutorState executorState, int immediateRetries, int maximumRetries) {
    SimpleLsfPipeline pipeline =
        new SimpleLsfPipeline(
            executorState == StageExecutorState.ERROR ? 1 : 0,
            immediateRetries,
            maximumRetries,
            lsfTestConfiguration);
    String processId = UniqueStringGenerator.randomProcessId(StageRunnerTest.class);
    ProcessBuilder processBuilder = new ProcessBuilder(processId);
    pipeline.configureProcess(processBuilder);
    return processBuilder.build();
  }

  private Process simulateProcessCreation(
      SingleStageProcessFactory singleStageProcessFactory,
      StageExecutorState executorState,
      String pipelineName,
      int immediateRetries,
      int maximumRetries) {
    Process process =
        singleStageProcessFactory.create(executorState, immediateRetries, maximumRetries);
    process.setProcessEntity(
        ProcessEntity.createExecution(
            pipelineName, process.getProcessId(), ProcessEntity.DEFAULT_PRIORITY));
    Stage stage = process.getStages().get(0);
    stage.setStageEntity(StageEntity.createExecution(pipelineName, process.getProcessId(), stage));
    return process;
  }

  private StageExecutorResult simulateSyncExecutionFirstIteration(
      Process process, StageRunner stageRunner) {
    process.getProcessEntity().startExecution();
    Stage stage = stageRunner.getStage();
    stage.getStageEntity().startExecution(stage);

    AtomicReference<StageExecutorResult> result = new AtomicReference<>();
    stageRunner.runOneIteration(r -> result.set(r));
    assertThat(result.get()).isNotNull();
    return result.get();
  }

  private void simulateAsyncExecutionFirstIteration(Process process, StageRunner stageRunner) {
    process.getProcessEntity().startExecution();
    Stage stage = stageRunner.getStage();
    stage.getStageEntity().startExecution(stage);

    AtomicReference<StageExecutorResult> result = new AtomicReference<>();
    stageRunner.runOneIteration(r -> result.set(r));
    assertThat(result.get()).isNull();
    assertThat(stageRunner.getExecutorResult().getExecutorState())
        .isEqualTo(StageExecutorState.SUBMITTED);
  }

  private StageExecutorResult simulateExecutionNextIterations(StageRunner stageRunner) {
    AtomicReference<StageExecutorResult> result = new AtomicReference<>();
    while (true) {
      stageRunner.runOneIteration(r -> result.set(r));
      if (result.get() != null && !result.get().isActive()) {
        if (!DependencyResolver.isImmediatelyExecutableStage(
            stageRunner.getProcess().getStages(),
            Collections.emptyList(),
            stageRunner.getStage())) {
          return result.get();
        }
      }
    }
  }

  private interface StageEntityAssert {
    void doAssert(String pipelineName, String processId, StageEntity stageEntity);
  }

  private void assertStageEntity(
      StageExecutorState executorState,
      String pipelineName,
      String processId,
      String stageName,
      StageEntityAssert stageEntityAssert) {
    // General asserts
    StageEntity stageEntity =
        pipeliteServices.stage().getSavedStage(pipelineName, processId, stageName).get();
    assertThat(stageEntity.getPipelineName()).isEqualTo(pipelineName);
    assertThat(stageEntity.getProcessId()).isEqualTo(processId);
    assertThat(stageEntity.getStageName()).isEqualTo(stageName);
    assertThat(stageEntity.getStageState()).isEqualTo(executorState.toStageState());
    if (stageEntity.getStageState() == StageState.ERROR) {
      assertThat(stageEntity.getExecutionCount()).isEqualTo(RETRY_CNT + 1);
    } else {
      assertThat(stageEntity.getExecutionCount()).isEqualTo(1);
    }
    assertThat(stageEntity.getStartTime()).isNotNull();
    assertThat(stageEntity.getEndTime()).isAfterOrEqualTo(stageEntity.getStartTime());
    assertThat(stageEntity.getExecutorParams()).contains("\"maximumRetries\" : " + RETRY_CNT);
    assertThat(stageEntity.getExecutorParams()).contains("\"immediateRetries\" : " + RETRY_CNT);

    // Executor specific asserts
    stageEntityAssert.doAssert(pipelineName, processId, stageEntity);
  }

  private void simulateSyncExecution(
      StageExecutorState executorState,
      int immediateRetries,
      int maximumRetries,
      SingleStageProcessFactory singleStageProcessFactory,
      StageEntityAssert stageEntityAssert) {
    String serviceName = UniqueStringGenerator.randomServiceName(StageRunnerTest.class);
    String pipelineName = UniqueStringGenerator.randomPipelineName(StageRunnerTest.class);

    Process process =
        simulateProcessCreation(
            singleStageProcessFactory,
            executorState,
            pipelineName,
            immediateRetries,
            maximumRetries);

    Stage stage = process.getStages().get(0);
    StageRunner stageRunner =
        spy(
            new StageRunner(
                pipeliteServices, pipeliteMetrics, serviceName, pipelineName, process, stage));

    // Run first iteration
    StageExecutorResult result = simulateSyncExecutionFirstIteration(process, stageRunner);

    // Run next iterations
    if (result.isError()) {
      result = simulateExecutionNextIterations(stageRunner);
    }

    assertThat(result.getExecutorState()).isEqualTo(executorState);
    assertThat(result.getStageState()).isEqualTo(executorState.toStageState());

    assertStageEntity(
        executorState,
        pipelineName,
        process.getProcessId(),
        stage.getStageName(),
        stageEntityAssert);
  }

  private void simulateAsyncExecution(
      StageExecutorState executorState,
      int immediateRetries,
      int maximumRetries,
      SingleStageProcessFactory singleStageProcessFactory,
      StageEntityAssert stageEntityAssert) {
    String serviceName = UniqueStringGenerator.randomServiceName(StageRunnerTest.class);
    String pipelineName = UniqueStringGenerator.randomPipelineName(StageRunnerTest.class);

    Process process =
        simulateProcessCreation(
            singleStageProcessFactory,
            executorState,
            pipelineName,
            immediateRetries,
            maximumRetries);

    Stage stage = process.getStages().get(0);
    StageRunner stageRunner =
        spy(
            new StageRunner(
                pipeliteServices, pipeliteMetrics, serviceName, pipelineName, process, stage));

    // Run first iteration
    simulateAsyncExecutionFirstIteration(process, stageRunner);

    assertThat(stage.getStageEntity().getStageState()).isEqualTo(StageState.ACTIVE);

    // Run next iterations
    StageExecutorResult result = simulateExecutionNextIterations(stageRunner);

    assertThat(result.getExecutorState()).isEqualTo(executorState);
    assertThat(result.getStageState()).isEqualTo(executorState.toStageState());

    assertStageEntity(
        executorState,
        pipelineName,
        process.getProcessId(),
        stage.getStageName(),
        stageEntityAssert);
  }

  @Test
  public void testExecutionUsingSyncTestExecutor() {
    for (StageExecutorState executorState :
        EnumSet.of(StageExecutorState.SUCCESS, StageExecutorState.ERROR)) {
      simulateSyncExecution(
          executorState,
          RETRY_CNT,
          RETRY_CNT,
          (executorState2, immediateRetries2, maximumRetries2) ->
              singleStageProcessSyncTestExecutorFactory(
                  executorState2, immediateRetries2, maximumRetries2),
          ((pipelineName, processId, stageEntity) -> {}));
    }
  }

  @Test
  public void testExecutionUsingAsyncTestExecutor() {
    for (StageExecutorState executorState :
        EnumSet.of(StageExecutorState.SUCCESS, StageExecutorState.ERROR)) {
      simulateAsyncExecution(
          executorState,
          RETRY_CNT,
          RETRY_CNT,
          (executorState2, immediateRetries2, maximumRetries2) ->
              singleStageProcessAsyncTestExecutorFactory(
                  executorState2, immediateRetries2, maximumRetries2),
          ((pipelineName, processId, stageEntity) -> {}));
    }
  }

  @Test
  // @Timeout(value = 60, unit = SECONDS)
  public void testExecutionUsingSimpleLsfExecutor() {
    for (StageExecutorState executorState :
        EnumSet.of(StageExecutorState.SUCCESS, StageExecutorState.ERROR)) {
      simulateAsyncExecution(
          executorState,
          RETRY_CNT,
          RETRY_CNT,
          (executorState2, immediateRetries2, maximumRetries2) ->
              singleStageProcessSimpleLsfExecutorFactory(
                  executorState2, immediateRetries2, maximumRetries2),
          ((pipelineName, processId, stageEntity) -> {
            // Executor specific assert
            boolean isError = stageEntity.getStageState().equals(StageState.ERROR);
            SimpleLsfExecutorTestHelper.TestType testType =
                isError
                    ? SimpleLsfExecutorTestHelper.TestType.NON_PERMANENT_ERROR
                    : SimpleLsfExecutorTestHelper.TestType.SUCCESS;
            SimpleLsfExecutorTestHelper.assertStageEntity(
                pipeliteServices.stage(),
                pipelineName,
                processId,
                stageEntity.getStageName(),
                testType,
                Collections.emptyList(),
                CreateProcessSingleStageSimpleLsfPipelineTestHelper.cmd(isError ? 1 : 0),
                isError ? 1 : 0,
                RETRY_CNT,
                RETRY_CNT);
          }));
    }
  }
}
