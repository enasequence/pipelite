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
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
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
import pipelite.helper.process.SingleStageAsyncTestProcessConfiguration;
import pipelite.helper.process.SingleStageSimpleLsfTestProcessConfiguration;
import pipelite.helper.process.SingleStageSyncTestProcessConfiguration;
import pipelite.helper.process.SingleStageTestProcessConfiguration;
import pipelite.metrics.PipeliteMetrics;
import pipelite.process.Process;
import pipelite.service.PipeliteServices;
import pipelite.stage.Stage;
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

  private ConfigurableTestPipeline<SingleStageTestProcessConfiguration>
      syncConfigurableTestPipeline(TestType testType) {
    return new ConfigurableTestPipeline(
        PARALLELISM,
        PROCESS_CNT,
        new SingleStageSyncTestProcessConfiguration(testType, IMMEDIATE_RETRIES, MAXIMUM_RETRIES));
  }

  private ConfigurableTestPipeline<SingleStageTestProcessConfiguration>
      asyncConfigurableTestPipeline(TestType testType) {
    return new ConfigurableTestPipeline(
        PARALLELISM,
        PROCESS_CNT,
        new SingleStageAsyncTestProcessConfiguration(testType, IMMEDIATE_RETRIES, MAXIMUM_RETRIES));
  }

  private ConfigurableTestPipeline<SingleStageTestProcessConfiguration>
      simpleLsfConfigurableTestPipeline(TestType testType) {
    return new ConfigurableTestPipeline(
        PARALLELISM,
        PROCESS_CNT,
        new SingleStageSimpleLsfTestProcessConfiguration(
            testType, IMMEDIATE_RETRIES, MAXIMUM_RETRIES, lsfTestConfiguration));
  }

  private Process simulateProcessCreation(
      ConfigurableTestPipeline<SingleStageTestProcessConfiguration> configurableTestPipeline,
      String pipelineName) {
    Process process = configurableTestPipeline.createProcess();
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
    assertThat(stageRunner.getExecutorResult().isSubmitted()).isTrue();
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
      ConfigurableTestPipeline<SingleStageTestProcessConfiguration> configurableTestPipeline,
      SimulateExecutionFirstIterationFactory simulateExecutionFirstIterationFactory) {
    String serviceName =
        UniqueStringGenerator.randomServiceName(StageRunnerDifferentExecutorsTest.class);

    String pipelineName = configurableTestPipeline.pipelineName();
    Process process = simulateProcessCreation(configurableTestPipeline, pipelineName);

    Stage stage = process.getStages().get(0);
    StageRunner stageRunner =
        spy(
            new StageRunner(
                pipeliteServices, pipeliteMetrics, serviceName, pipelineName, process, stage));

    // Run first iteration
    StageExecutorResult result =
        simulateExecutionFirstIterationFactory.create(process, stageRunner);

    if (result.isSubmitted()) {
      configurableTestPipeline
          .getRegisteredPipeline()
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

    // ProcessEntity is not created nor asserted in this test

    configurableTestPipeline
        .getRegisteredPipeline()
        .assertCompletedStageEntities(pipeliteServices.stage(), PROCESS_CNT);
  }

  @Test
  public void testExecutionUsingSyncTestExecutor() {
    for (TestType testType : EnumSet.of(TestType.NON_PERMANENT_ERROR, TestType.SUCCESS)) {
      ConfigurableTestPipeline<SingleStageTestProcessConfiguration> configurableTestPipeline =
          syncConfigurableTestPipeline(testType);
      simulateExecution(
          testType,
          configurableTestPipeline,
          (process, stageRunner) -> simulateSyncExecutionFirstIteration(process, stageRunner));
    }
  }

  @Test
  public void testExecutionUsingAsyncTestExecutor() {
    for (TestType testType : EnumSet.of(TestType.NON_PERMANENT_ERROR, TestType.SUCCESS)) {
      ConfigurableTestPipeline<SingleStageTestProcessConfiguration> processFactory =
          asyncConfigurableTestPipeline(testType);
      simulateExecution(
          testType,
          processFactory,
          (process, stageRunner) -> simulateAsyncExecutionFirstIteration(process, stageRunner));
    }
  }

  @Test
  @EnabledIfEnvironmentVariable(named = "PIPELITE_TEST_LSF_HOST", matches = ".+")
  // @Timeout(value = 60, unit = SECONDS)
  public void testExecutionUsingSimpleLsfExecutor() {
    for (TestType testType : EnumSet.of(TestType.NON_PERMANENT_ERROR, TestType.SUCCESS)) {
      ConfigurableTestPipeline<SingleStageTestProcessConfiguration> configurableTestPipeline =
          simpleLsfConfigurableTestPipeline(testType);
      simulateExecution(
          testType,
          configurableTestPipeline,
          (process, stageRunner) -> simulateAsyncExecutionFirstIteration(process, stageRunner));
    }
  }
}
