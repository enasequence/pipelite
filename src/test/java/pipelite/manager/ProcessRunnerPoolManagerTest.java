/*
 * Copyright 2020-2022 EMBL - European Bioinformatics Institute
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package pipelite.manager;

import static org.assertj.core.api.Assertions.assertThat;
import static pipelite.PipeliteTestConstants.CRON_EVERY_TWO_SECONDS;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Profile;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import pipelite.Pipeline;
import pipelite.PipeliteTestConfigWithManager;
import pipelite.Schedule;
import pipelite.metrics.PipeliteMetrics;
import pipelite.process.builder.ProcessBuilder;
import pipelite.runner.schedule.ScheduleRunner;
import pipelite.service.RunnerService;
import pipelite.stage.executor.StageExecutorState;
import pipelite.tester.pipeline.ConfigurableTestPipeline;
import pipelite.tester.pipeline.ConfigurableTestSchedule;
import pipelite.tester.process.TestProcessConfiguration;

@SpringBootTest(
    classes = PipeliteTestConfigWithManager.class,
    properties = {
      "pipelite.advanced.processRunnerFrequency=250ms",
      "pipelite.advanced.shutdownIfIdle=true",
      "pipelite.service.force=true",
      "pipelite.service.name=ProcessRunnerPoolManagerTest"
    })
@ActiveProfiles({"test", "ProcessRunnerPoolManagerTest"})
@DirtiesContext
public class ProcessRunnerPoolManagerTest {

  private static final int PARALLELISM = 1;
  private static final int PROCESS_CNT = 1;
  private static final String STAGE_NAME = "STAGE";

  @Autowired ProcessRunnerPoolManager processRunnerPoolManager;
  @Autowired RunnerService runnerService;
  @Autowired PipeliteMetrics pipeliteMetrics;
  @Autowired Schedule testSchedule;
  @Autowired Pipeline testPipeline;

  @Profile("ProcessRunnerPoolManagerTest")
  @TestConfiguration
  static class TestConfig {

    @Bean
    Schedule testSchedule() {
      return new ConfigurableTestSchedule(
          CRON_EVERY_TWO_SECONDS,
          new TestProcessConfiguration() {
            @Override
            public void configureProcess(ProcessBuilder builder) {
              builder.execute(STAGE_NAME).withSyncTestExecutor(StageExecutorState.SUCCESS);
            }
          });
    }

    @Bean
    Pipeline testPipeline() {
      return new ConfigurableTestPipeline(
          PARALLELISM,
          PROCESS_CNT,
          new TestProcessConfiguration() {
            @Override
            public void configureProcess(ProcessBuilder builder) {
              builder.execute(STAGE_NAME).withSyncTestExecutor(StageExecutorState.SUCCESS);
            }
          });
    }
  }

  @Test
  public void test() {
    processRunnerPoolManager.createPools();

    assertThat(runnerService.isScheduleRunner()).isTrue();
    assertThat(runnerService.getPipelineRunners().size()).isEqualTo(1);

    ScheduleRunner scheduleRunner = runnerService.getScheduleRunner();
    scheduleRunner.setMaximumExecutions(testSchedule.pipelineName(), PROCESS_CNT);

    processRunnerPoolManager.startPools();
    processRunnerPoolManager.waitPoolsToStop();

    assertThat(
            pipeliteMetrics
                .process(testSchedule.pipelineName())
                .stage(STAGE_NAME)
                .runner()
                .successCount())
        .isEqualTo(1);
    assertThat(
            pipeliteMetrics
                .process(testPipeline.pipelineName())
                .stage(STAGE_NAME)
                .runner()
                .successCount())
        .isEqualTo(1);
  }
}
