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
import javax.annotation.PostConstruct;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Profile;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import pipelite.PipeliteTestConfigWithManager;
import pipelite.manager.ProcessRunnerPoolManager;
import pipelite.metrics.PipeliteMetrics;
import pipelite.service.ProcessService;
import pipelite.service.RunnerService;
import pipelite.service.StageService;
import pipelite.tester.TestType;
import pipelite.tester.TestTypeConfiguration;
import pipelite.tester.pipeline.ConfigurableTestPipeline;
import pipelite.tester.process.SingleStageCmdTestProcessConfiguration;
import pipelite.tester.process.SingleStageTestProcessConfiguration;

@SpringBootTest(
    classes = PipeliteTestConfigWithManager.class,
    properties = {
      "pipelite.service.force=true",
      "pipelite.service.name=PipelineRunnerTest",
      "pipelite.advanced.processRunnerFrequency=250ms",
      "pipelite.advanced.shutdownIfIdle=true"
    })
@ActiveProfiles({"test", "PipelineRunnerCmdExecutorTest"})
@DirtiesContext
public class PipelineRunnerCmdExecutorTest {

  private static final int PROCESS_CNT = 10;
  private static final int IMMEDIATE_RETRIES = 3;
  private static final int MAXIMUM_RETRIES = 3;
  private static final int PARALLELISM = 10;

  @Autowired private ProcessRunnerPoolManager processRunnerPoolManager;
  @Autowired private ProcessService processService;
  @Autowired private RunnerService runnerService;
  @Autowired private PipeliteMetrics metrics;

  @Autowired
  private List<ConfigurableTestPipeline<SingleStageTestProcessConfiguration>> testPipelines;

  // TestTypeConfiguration ->
  @SpyBean private StageService stageService;

  @PostConstruct
  public void init() {
    TestTypeConfiguration.init(stageService);
  }

  private static TestTypeConfiguration testTypeConfiguration(TestType testType) {
    return new TestTypeConfiguration(testType, IMMEDIATE_RETRIES, MAXIMUM_RETRIES);
  }
  // <- TestTypeConfiguration

  @Profile("PipelineRunnerCmdExecutorTest")
  @TestConfiguration
  static class TestConfig {
    @Bean
    public SingleStageTestPipeline singleStageTestSuccessPipeline() {
      return new SingleStageTestPipeline(TestType.SUCCESS);
    }

    @Bean
    public SingleStageTestPipeline singleStageTestSuccessAfterOneNonPermanentErrorPipeline() {
      return new SingleStageTestPipeline(TestType.SUCCESS_AFTER_ONE_NON_PERMANENT_ERROR);
    }

    @Bean
    public SingleStageTestPipeline singleStageTestNonPermanentErrorPipeline() {
      return new SingleStageTestPipeline(TestType.NON_PERMANENT_ERROR);
    }

    @Bean
    public SingleStageTestPipeline singleStageTestPermanentErrorPipeline() {
      return new SingleStageTestPipeline(TestType.PERMANENT_ERROR);
    }
  }

  private static class SingleStageTestPipeline extends ConfigurableTestPipeline {
    public SingleStageTestPipeline(TestType testType) {
      super(
          PARALLELISM,
          PROCESS_CNT,
          new SingleStageCmdTestProcessConfiguration(testTypeConfiguration(testType)));
    }
  }

  private void assertPipeline(ConfigurableTestPipeline<SingleStageTestProcessConfiguration> f) {
    for (PipelineRunner pipelineRunner : runnerService.getPipelineRunners()) {
      assertThat(pipelineRunner.getActiveProcessRunners().size()).isEqualTo(0);
    }
    f.assertCompleted(processService, stageService, metrics);
  }

  @Test
  public void runPipelines() {
    processRunnerPoolManager.createPools();
    processRunnerPoolManager.startPools();
    processRunnerPoolManager.waitPoolsToStop();

    for (ConfigurableTestPipeline<SingleStageTestProcessConfiguration> f : testPipelines) {
      assertPipeline(f);
    }
  }
}
