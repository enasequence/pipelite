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
import pipelite.helper.*;
import pipelite.manager.ProcessRunnerPoolManager;
import pipelite.metrics.PipeliteMetrics;
import pipelite.service.ProcessService;
import pipelite.service.RunnerService;
import pipelite.service.StageService;

@SpringBootTest(
    classes = PipeliteTestConfigWithManager.class,
    properties = {
      "pipelite.service.force=true",
      "pipelite.service.name=PipelineRunnerTest",
      "pipelite.advanced.processRunnerFrequency=250ms",
      "pipelite.advanced.shutdownIfIdle=true"
    })
@ActiveProfiles({"test", "PipelineRunnerTestExecutorTest"})
@DirtiesContext
public class PipelineRunnerTestExecutorTest {

  private static final int PROCESS_CNT = 2;
  private static final int IMMEDIATE_RETRIES = 3;
  private static final int MAXIMUM_RETRIES = 3;
  private static final int PARALLELISM = 1;

  @Autowired private ProcessRunnerPoolManager processRunnerPoolManager;
  @Autowired private ProcessService processService;
  @Autowired private StageService stageService;
  @Autowired private RunnerService runnerService;
  @Autowired private PipeliteMetrics metrics;

  @Autowired
  private List<RegisteredTestPipelineWrappingPipeline<RegisteredSingleStageTestPipeline>>
      testPipelines;

  @Profile("PipelineRunnerTestExecutorTest")
  @TestConfiguration
  static class TestConfig {
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
  }

  private static class SimpleSyncTestPipeline extends RegisteredTestPipelineWrappingPipeline {
    public SimpleSyncTestPipeline(TestType testType) {
      super(
          PARALLELISM,
          PROCESS_CNT,
          new RegisteredSingleStageSyncTestPipeline(testType, IMMEDIATE_RETRIES, MAXIMUM_RETRIES));
    }
  }

  private static class SimpleAsyncTestPipeline extends RegisteredTestPipelineWrappingPipeline {
    public SimpleAsyncTestPipeline(TestType testType) {
      super(
          PARALLELISM,
          PROCESS_CNT,
          new RegisteredSingleStageAsyncTestPipeline(testType, IMMEDIATE_RETRIES, MAXIMUM_RETRIES));
    }
  }

  private void assertPipeline(
      RegisteredTestPipelineWrappingPipeline<RegisteredSingleStageTestPipeline> f) {
    for (PipelineRunner pipelineRunner : runnerService.getPipelineRunners()) {
      assertThat(pipelineRunner.getActiveProcessRunners().size()).isEqualTo(0);
    }
    RegisteredSingleStageTestPipeline registeredTestPipeline = f.getRegisteredTestPipeline();
    assertThat(registeredTestPipeline.configuredProcessIds().size()).isEqualTo(PROCESS_CNT);
    registeredTestPipeline.assertCompletedMetrics(metrics, PROCESS_CNT);
    registeredTestPipeline.assertCompletedProcessEntities(processService, PROCESS_CNT);
    registeredTestPipeline.assertCompletedStageEntities(stageService, PROCESS_CNT);
  }

  @Test
  public void runPipelines() {
    processRunnerPoolManager.createPools();
    processRunnerPoolManager.startPools();
    processRunnerPoolManager.waitPoolsToStop();

    for (RegisteredTestPipelineWrappingPipeline<RegisteredSingleStageTestPipeline> f :
        testPipelines) {
      assertPipeline(f);
    }
  }
}
