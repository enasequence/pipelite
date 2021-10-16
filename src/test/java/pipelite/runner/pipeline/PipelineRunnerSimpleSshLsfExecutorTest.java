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
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Profile;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import pipelite.PipeliteTestConfigWithManager;
import pipelite.configuration.properties.LsfTestConfiguration;
import pipelite.helper.RegisteredSingleStageSimpleLsfTestPipeline;
import pipelite.helper.RegisteredSingleStageTestPipeline;
import pipelite.helper.RegisteredTestPipelineWrappingPipeline;
import pipelite.helper.TestType;
import pipelite.manager.ProcessRunnerPoolManager;
import pipelite.metrics.PipeliteMetrics;
import pipelite.service.ProcessService;
import pipelite.service.RunnerService;
import pipelite.service.StageService;
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
public class PipelineRunnerSimpleSshLsfExecutorTest {

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

  @Profile("PipelineRunnerSimpleSshLsfExecutorTest")
  @TestConfiguration
  static class TestConfig {
    @Autowired private LsfTestConfiguration lsfTestConfiguration;

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

  private static int getExitCode(TestType testType) {
    return testType == TestType.NON_PERMANENT_ERROR ? 1 : 0;
  }

  private static class SimpleLsfPipeline extends RegisteredTestPipelineWrappingPipeline {
    public SimpleLsfPipeline(TestType testType, LsfTestConfiguration lsfTestConfiguration) {
      super(
          PARALLELISM,
          PROCESS_CNT,
          new RegisteredSingleStageSimpleLsfTestPipeline(
              testType,
              getExitCode(testType),
              IMMEDIATE_RETRIES,
              MAXIMUM_RETRIES,
              lsfTestConfiguration));
    }
  }

  private static class SimpleLsfPermanentErrorPipeline
      extends RegisteredTestPipelineWrappingPipeline {
    public SimpleLsfPermanentErrorPipeline(LsfTestConfiguration lsfTestConfiguration) {
      super(
          PARALLELISM,
          PROCESS_CNT,
          new RegisteredSingleStageSimpleLsfTestPipeline(
              TestType.PERMANENT_ERROR,
              getExitCode(TestType.PERMANENT_ERROR),
              IMMEDIATE_RETRIES,
              MAXIMUM_RETRIES,
              lsfTestConfiguration) {
            @Override
            protected void testExecutorParams(
                SimpleLsfExecutorParameters.SimpleLsfExecutorParametersBuilder<?, ?>
                    executorParamsBuilder) {
              executorParamsBuilder.permanentError(0);
            }
          });
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
  @EnabledIfEnvironmentVariable(named = "PIPELITE_TEST_LSF_HOST", matches = ".+")
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