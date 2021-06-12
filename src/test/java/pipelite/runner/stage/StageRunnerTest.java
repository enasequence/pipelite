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

import java.util.Collections;
import java.util.EnumSet;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
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
import pipelite.helper.StageEntityTestHelper;
import pipelite.helper.TestType;
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

  private Process createSingleStageProcessSyncTestExecutor(
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

  private Process createSingleStageProcessAsyncTestExecutor(
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

  private Process createSingleStageProcessSimpleLsfExecutor(
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

  private interface SimulateExecutionFirstIterationFactory {
    StageExecutorResult create(Process process, StageRunner stageRunner);
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

  private StageExecutorResult simulateAsyncExecutionFirstIteration(
      Process process, StageRunner stageRunner) {
    process.getProcessEntity().startExecution();
    Stage stage = stageRunner.getStage();
    stage.getStageEntity().startExecution(stage);

    AtomicReference<StageExecutorResult> result = new AtomicReference<>();
    stageRunner.runOneIteration(r -> result.set(r));
    assertThat(result.get()).isNull();
    assertThat(stageRunner.getExecutorResult().getExecutorState())
        .isEqualTo(StageExecutorState.SUBMITTED);
    assertThat(stageRunner.getExecutorResult().getStageState()).isEqualTo(StageState.ACTIVE);
    return StageExecutorResult.submitted();
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

  private void simulateExecution(
      StageExecutorState executorState,
      int immediateRetries,
      int maximumRetries,
      SingleStageProcessFactory singleStageProcessFactory,
      SimulateExecutionFirstIterationFactory simulateExecutionFirstIterationFactory,
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
    StageExecutorResult result =
        simulateExecutionFirstIterationFactory.create(process, stageRunner);

    // Run next iterations
    if (result.isSubmitted()
        || (result.isError()
            && DependencyResolver.isImmediatelyExecutableStage(
                stageRunner.getProcess().getStages(),
                Collections.emptyList(),
                stageRunner.getStage()))) {
      result = simulateExecutionNextIterations(stageRunner);
    }

    assertThat(result.getExecutorState()).isEqualTo(executorState);
    assertThat(result.getStageState()).isEqualTo(executorState.toStageState());

    TestType testType = result.isError() ? TestType.NON_PERMANENT_ERROR : TestType.SUCCESS;

    // ProcessEntity is not created in this test

    stageEntityAssert.assertStageEntity(
        pipelineName, process.getProcessId(), stage.getStageName(), testType);
  }

  private interface StageEntityAssert {
    void assertStageEntity(
        String pipelineName, String processId, String stageName, TestType testType);
  }

  @Test
  public void testExecutionUsingSyncTestExecutor() {
    for (StageExecutorState executorState :
        EnumSet.of(StageExecutorState.SUCCESS, StageExecutorState.ERROR)) {
      simulateExecution(
          executorState,
          RETRY_CNT,
          RETRY_CNT,
          (executorState2, immediateRetries, maximumRetries) ->
              createSingleStageProcessSyncTestExecutor(
                  executorState2, immediateRetries, maximumRetries),
          (process, stageRunner) -> simulateSyncExecutionFirstIteration(process, stageRunner),
          ((pipelineName, processId, stageName, testType) ->
              StageEntityTestHelper.assertTestExecutorStageEntity(
                  pipeliteServices.stage(),
                  pipelineName,
                  processId,
                  stageName,
                  testType,
                  RETRY_CNT,
                  RETRY_CNT)));
    }
  }

  @Test
  public void testExecutionUsingAsyncTestExecutor() {
    for (StageExecutorState executorState :
        EnumSet.of(StageExecutorState.SUCCESS, StageExecutorState.ERROR)) {
      simulateExecution(
          executorState,
          RETRY_CNT,
          RETRY_CNT,
          (executorState2, immediateRetries, maximumRetries) ->
              createSingleStageProcessAsyncTestExecutor(
                  executorState2, immediateRetries, maximumRetries),
          (process, stageRunner) -> simulateAsyncExecutionFirstIteration(process, stageRunner),
          ((pipelineName, processId, stageName, testType) ->
              StageEntityTestHelper.assertTestExecutorStageEntity(
                  pipeliteServices.stage(),
                  pipelineName,
                  processId,
                  stageName,
                  testType,
                  RETRY_CNT,
                  RETRY_CNT)));
    }
  }

  @Test
  // @Timeout(value = 60, unit = SECONDS)
  public void testExecutionUsingSimpleLsfExecutor() {
    for (StageExecutorState executorState :
        EnumSet.of(StageExecutorState.SUCCESS, StageExecutorState.ERROR)) {
      simulateExecution(
          executorState,
          RETRY_CNT,
          RETRY_CNT,
          (executorState2, immediateRetries2, maximumRetries2) ->
              createSingleStageProcessSimpleLsfExecutor(
                  executorState2, immediateRetries2, maximumRetries2),
          (process, stageRunner) -> simulateAsyncExecutionFirstIteration(process, stageRunner),
          ((pipelineName, processId, stageName, testType) -> {
            boolean isError = testType != TestType.SUCCESS;
            StageEntityTestHelper.assertSimpleLsfExecutorStageEntity(
                pipeliteServices.stage(),
                pipelineName,
                processId,
                stageName,
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
