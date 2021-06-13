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
package pipelite.runner.pipeline;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Profile;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import pipelite.PipeliteTestConfigWithManager;
import pipelite.configuration.properties.LsfTestConfiguration;
import pipelite.helper.*;
import pipelite.manager.ProcessRunnerPoolManager;
import pipelite.metrics.PipeliteMetrics;
import pipelite.service.ProcessService;
import pipelite.service.RunnerService;
import pipelite.service.StageService;
import pipelite.stage.executor.StageExecutorState;
import pipelite.stage.parameters.SimpleLsfExecutorParameters;

@SpringBootTest(
    classes = PipeliteTestConfigWithManager.class,
    properties = {
      "pipelite.service.force=true",
      "pipelite.service.name=PipelineRunnerTest",
      "pipelite.advanced.processRunnerFrequency=250ms",
      "pipelite.advanced.shutdownIfIdle=true"
    })
@ActiveProfiles({"test", "PipelineRunnerSimpleSshLsfExecutorTest"})
@DirtiesContext
public class PipelineRunnerDifferentExecutorsTest {

  private static final int PROCESS_CNT = 2;
  private static final int IMMEDIATE_RETRIES = 3;
  private static final int MAXIMUM_RETRIES = 3;
  private static final int PARALLELISM = 1;

  @Autowired private ProcessRunnerPoolManager processRunnerPoolManager;
  @Autowired private ProcessService processService;
  @Autowired private StageService stageService;
  @Autowired private RunnerService runnerService;
  @Autowired private PipeliteMetrics metrics;

  @Autowired private List<SingleStageTestProcessFactory> testPipelines;

  @Profile("PipelineRunnerSimpleSshLsfExecutorTest")
  @TestConfiguration
  static class TestConfig {
    @Autowired private LsfTestConfiguration lsfTestConfiguration;

    @Bean
    public SimpleSyncTestPipeline simpleSyncTestSuccessPipeline() {
      return new SimpleSyncTestPipeline(TestType.SUCCESS);
    }

    @Bean
    public SimpleSyncTestPipeline simpleSyncTestSNonPermanentErrorPipeline() {
      return new SimpleSyncTestPipeline(TestType.NON_PERMANENT_ERROR);
    }

    @Bean
    public SimpleAsyncTestPipeline simpleAsyncTestSuccessPipeline() {
      return new SimpleAsyncTestPipeline(TestType.SUCCESS);
    }

    @Bean
    public SimpleAsyncTestPipeline simpleAsyncTestSNonPermanentErrorPipeline() {
      return new SimpleAsyncTestPipeline(TestType.NON_PERMANENT_ERROR);
    }

    @Bean
    public SimpleLsfPipeline simpleLsfSuccessPipeline() {
      return new SimpleLsfPipeline(TestType.SUCCESS, lsfTestConfiguration);
    }

    @Bean
    public SimpleLsfPipeline simpleLsfNonPermanentErrorPipeline() {
      return new SimpleLsfPipeline(TestType.NON_PERMANENT_ERROR, lsfTestConfiguration);
    }

    @Bean
    public SimpleLsfPermanentErrorPipeline simpleLsfPermanentErrorPipeline() {
      return new SimpleLsfPermanentErrorPipeline(lsfTestConfiguration);
    }
  }

  private static StageExecutorState getCompletedExecutorState(TestType testType) {
    return testType == TestType.NON_PERMANENT_ERROR
        ? StageExecutorState.ERROR
        : StageExecutorState.SUCCESS;
  }

  private static int getExitCode(TestType testType) {
    return testType == TestType.NON_PERMANENT_ERROR ? 1 : 0;
  }

  private static class SimpleSyncTestPipeline extends SingleStageSyncTestProcessFactory {
    public SimpleSyncTestPipeline(TestType testType) {
      super(
          testType,
          PROCESS_CNT,
          PARALLELISM,
          PipelineRunnerDifferentExecutorsTest.getCompletedExecutorState(testType),
          IMMEDIATE_RETRIES,
          MAXIMUM_RETRIES);
    }
  }

  private static class SimpleAsyncTestPipeline extends SingleStageAsyncTestProcessFactory {
    public SimpleAsyncTestPipeline(TestType testType) {
      super(
          testType,
          PROCESS_CNT,
          PARALLELISM,
          PipelineRunnerDifferentExecutorsTest.getCompletedExecutorState(testType),
          IMMEDIATE_RETRIES,
          MAXIMUM_RETRIES);
    }
  }

  private static class SimpleLsfPipeline extends SingleStageSimpleLsfTestProcessFactory {
    public SimpleLsfPipeline(TestType testType, LsfTestConfiguration lsfTestConfiguration) {
      super(
          testType,
          PROCESS_CNT,
          PARALLELISM,
          getExitCode(testType),
          IMMEDIATE_RETRIES,
          MAXIMUM_RETRIES,
          lsfTestConfiguration);
    }
  }

  private static class SimpleLsfPermanentErrorPipeline extends SimpleLsfPipeline {
    public SimpleLsfPermanentErrorPipeline(LsfTestConfiguration lsfTestConfiguration) {
      super(TestType.PERMANENT_ERROR, lsfTestConfiguration);
    }

    @Override
    protected void testExecutorParams(
        SimpleLsfExecutorParameters.SimpleLsfExecutorParametersBuilder<?, ?>
            executorParamsBuilder) {
      executorParamsBuilder.permanentError(0);
    }
  }

  private void assertPipeline(SingleStageTestProcessFactory f) {
    for (PipelineRunner pipelineRunner : runnerService.getPipelineRunners()) {
      assertThat(pipelineRunner.getActiveProcessRunners().size()).isEqualTo(0);
    }
    assertThat(f.configuredProcessIds().size()).isEqualTo(PROCESS_CNT);
    f.assertCompletedMetrics(metrics);
    for (String processId : f.configuredProcessIds()) {
      f.assertCompletedProcessEntity(processService, processId);
      f.assertCompletedStageEntity(stageService, processId);
    }
  }

  @Test
  public void runPipelines() {
    processRunnerPoolManager.createPools();
    processRunnerPoolManager.startPools();
    processRunnerPoolManager.waitPoolsToStop();

    for (SingleStageTestProcessFactory f : testPipelines) {
      assertPipeline(f);
    }
  }
}
