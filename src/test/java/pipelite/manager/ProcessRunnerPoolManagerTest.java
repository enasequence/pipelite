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
package pipelite.manager;

import static org.assertj.core.api.Assertions.assertThat;
import static pipelite.PipeliteTestConstants.CRON_EVERY_TWO_SECONDS;

import java.util.concurrent.atomic.AtomicInteger;
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
import pipelite.helper.RegisteredConfiguredTestPipeline;
import pipelite.helper.RegisteredTestPipelineWrappingPipeline;
import pipelite.helper.RegisteredTestPipelineWrappingSchedule;
import pipelite.process.builder.ProcessBuilder;
import pipelite.runner.schedule.ScheduleRunner;
import pipelite.service.RunnerService;
import pipelite.stage.executor.StageExecutorResult;

@SpringBootTest(
    classes = PipeliteTestConfigWithManager.class,
    properties = {
      "pipelite.advanced.processRunnerFrequency=250ms",
      "pipelite.advanced.shutdownIfIdle=true",
      "pipelite.service.force=true",
      "pipelite.service.name=RegisteredServiceManagerTest"
    })
@ActiveProfiles({"test", "ProcessRunnerPoolManagerTest"})
@DirtiesContext
public class ProcessRunnerPoolManagerTest {

  private static final int PARALLELISM = 1;
  private static final int PROCESS_CNT = 1;

  private static final AtomicInteger scheduleExecutionCount = new AtomicInteger();
  private static final AtomicInteger pipelineExecutionCount = new AtomicInteger();

  @Autowired ProcessRunnerPoolManager processRunnerPoolManager;
  @Autowired RunnerService runnerService;

  @Autowired Schedule testSchedule;

  @Profile("ProcessRunnerPoolManagerTest")
  @TestConfiguration
  static class TestConfig {

    @Bean
    Schedule testSchedule() {
      return new RegisteredTestPipelineWrappingSchedule(
          CRON_EVERY_TWO_SECONDS,
          new RegisteredConfiguredTestPipeline() {
            @Override
            protected void testConfigureProcess(ProcessBuilder builder) {
              builder
                  .execute("STAGE")
                  .withSyncTestExecutor(
                      request -> {
                        scheduleExecutionCount.incrementAndGet();
                        return StageExecutorResult.success();
                      });
            }
          });
    }

    @Bean
    Pipeline testPipeline() {
      return new RegisteredTestPipelineWrappingPipeline(
          PARALLELISM,
          PROCESS_CNT,
          new RegisteredConfiguredTestPipeline() {
            @Override
            protected void testConfigureProcess(ProcessBuilder builder) {
              builder
                  .execute("STAGE")
                  .withSyncTestExecutor(
                      request -> {
                        pipelineExecutionCount.incrementAndGet();
                        return StageExecutorResult.success();
                      });
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

    assertThat(scheduleExecutionCount.get()).isEqualTo(1);
    assertThat(pipelineExecutionCount.get()).isEqualTo(1);
  }
}
