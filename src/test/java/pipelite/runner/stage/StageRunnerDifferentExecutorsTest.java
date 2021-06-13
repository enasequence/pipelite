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
import pipelite.helper.*;
import pipelite.metrics.PipeliteMetrics;
import pipelite.process.Process;
import pipelite.service.PipeliteServices;
import pipelite.stage.Stage;
import pipelite.stage.StageState;
import pipelite.stage.executor.StageExecutorResult;
import pipelite.stage.executor.StageExecutorState;

@SpringBootTest(
    classes = PipeliteTestConfigWithManager.class,
    properties = {"pipelite.service.force=true", "pipelite.service.name=StageRunnerTest"})
@ActiveProfiles({"test"})
@DirtiesContext
public class StageRunnerDifferentExecutorsTest {

  @Autowired PipeliteServices pipeliteServices;
  @Autowired PipeliteMetrics pipeliteMetrics;
  @Autowired LsfTestConfiguration lsfTestConfiguration;

  private static final int IMMEDIATE_RETRIES = 1;
  private static final int MAXIMUM_RETRIES = 1;
  private static final int PROCESS_CNT = 1;
  private static final int PARALLELISM = 1;

  private StageExecutorState getCompletedExecutorState(TestType testType) {
    return testType == TestType.NON_PERMANENT_ERROR
        ? StageExecutorState.ERROR
        : StageExecutorState.SUCCESS;
  }

  private int getExitCode(boolean isError) {
    return isError ? 1 : 0;
  }

  private RegisteredTestPipelineWrappingPipeline<RegisteredSingleStageTestPipeline>
      syncTestProcessFactory(TestType testType) {
    return new RegisteredTestPipelineWrappingPipeline(
        PARALLELISM,
        PROCESS_CNT,
        new RegisteredSingleStageSyncTestPipeline(testType, IMMEDIATE_RETRIES, MAXIMUM_RETRIES));
  }

  private RegisteredTestPipelineWrappingPipeline<RegisteredSingleStageTestPipeline>
      asyncTestProcessFactory(TestType testType) {
    return new RegisteredTestPipelineWrappingPipeline(
        PARALLELISM,
        PROCESS_CNT,
        new RegisteredSingleStageAsyncTestPipeline(testType, IMMEDIATE_RETRIES, MAXIMUM_RETRIES));
  }

  private RegisteredTestPipelineWrappingPipeline<RegisteredSingleStageTestPipeline>
      simpleLsfProcessFactory(TestType testType) {
    return new RegisteredTestPipelineWrappingPipeline(
        PARALLELISM,
        PROCESS_CNT,
        new RegisteredSingleStageSimpleLsfTestPipeline(
            testType,
            getExitCode(testType == TestType.NON_PERMANENT_ERROR),
            IMMEDIATE_RETRIES,
            MAXIMUM_RETRIES,
            lsfTestConfiguration));
  }

  private Process simulateProcessCreation(
      RegisteredTestPipelineWrappingPipeline<RegisteredSingleStageTestPipeline> processFactory,
      String pipelineName) {
    Process process = processFactory.createProcess();
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
      TestType testType,
      RegisteredTestPipelineWrappingPipeline<RegisteredSingleStageTestPipeline> processFactory,
      SimulateExecutionFirstIterationFactory simulateExecutionFirstIterationFactory) {
    String serviceName =
        UniqueStringGenerator.randomServiceName(StageRunnerDifferentExecutorsTest.class);

    String pipelineName = processFactory.pipelineName();
    Process process = simulateProcessCreation(processFactory, pipelineName);

    Stage stage = process.getStages().get(0);
    StageRunner stageRunner =
        spy(
            new StageRunner(
                pipeliteServices, pipeliteMetrics, serviceName, pipelineName, process, stage));

    // Run first iteration
    StageExecutorResult result =
        simulateExecutionFirstIterationFactory.create(process, stageRunner);

    if (result.isSubmitted()) {
      processFactory
          .getRegisteredTestPipeline()
          .assertSubmittedStageEntities(pipeliteServices.stage(), PROCESS_CNT);
    }

    // Run next iterations
    if (result.isSubmitted()
        || (result.isError()
            && DependencyResolver.isImmediatelyExecutableStage(
                stageRunner.getProcess().getStages(),
                Collections.emptyList(),
                stageRunner.getStage()))) {
      result = simulateExecutionNextIterations(stageRunner);
    }

    StageExecutorState executorState = getCompletedExecutorState(testType);
    assertThat(result.getExecutorState()).isEqualTo(executorState);
    assertThat(result.getStageState()).isEqualTo(executorState.toStageState());

    // ProcessEntity is not created nor asserted in this test

    processFactory
        .getRegisteredTestPipeline()
        .assertCompletedStageEntities(pipeliteServices.stage(), PROCESS_CNT);
  }

  @Test
  public void testExecutionUsingSyncTestExecutor() {
    for (TestType testType : EnumSet.of(TestType.NON_PERMANENT_ERROR, TestType.SUCCESS)) {
      RegisteredTestPipelineWrappingPipeline<RegisteredSingleStageTestPipeline> processFactory =
          syncTestProcessFactory(testType);
      simulateExecution(
          testType,
          processFactory,
          (process, stageRunner) -> simulateSyncExecutionFirstIteration(process, stageRunner));
    }
  }

  @Test
  public void testExecutionUsingAsyncTestExecutor() {
    for (TestType testType : EnumSet.of(TestType.NON_PERMANENT_ERROR, TestType.SUCCESS)) {
      RegisteredTestPipelineWrappingPipeline<RegisteredSingleStageTestPipeline> processFactory =
          asyncTestProcessFactory(testType);
      simulateExecution(
          testType,
          processFactory,
          (process, stageRunner) -> simulateAsyncExecutionFirstIteration(process, stageRunner));
    }
  }

  @Test
  // @Timeout(value = 60, unit = SECONDS)
  public void testExecutionUsingSimpleLsfExecutor() {
    for (TestType testType : EnumSet.of(TestType.NON_PERMANENT_ERROR, TestType.SUCCESS)) {
      RegisteredTestPipelineWrappingPipeline<RegisteredSingleStageTestPipeline> processFactory =
          simpleLsfProcessFactory(testType);
      simulateExecution(
          testType,
          processFactory,
          (process, stageRunner) -> simulateAsyncExecutionFirstIteration(process, stageRunner));
    }
  }
}
