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
package pipelite.runner.schedule;

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
import pipelite.configuration.ServiceConfiguration;
import pipelite.configuration.properties.LsfTestConfiguration;
import pipelite.entity.ScheduleEntity;
import pipelite.manager.ProcessRunnerPoolManager;
import pipelite.metrics.PipeliteMetrics;
import pipelite.service.ProcessService;
import pipelite.service.RunnerService;
import pipelite.service.ScheduleService;
import pipelite.service.StageService;
import pipelite.stage.parameters.SimpleLsfExecutorParameters;
import pipelite.tester.TestType;
import pipelite.tester.pipeline.ConfigurableTestSchedule;
import pipelite.tester.process.SingleStageSimpleLsfTestProcessConfiguration;
import pipelite.tester.process.SingleStageTestProcessConfiguration;

@SpringBootTest(
    classes = PipeliteTestConfigWithManager.class,
    properties = {
      "pipelite.service.force=true",
      "pipelite.service.name=ScheduleRunnerTest",
      "pipelite.advanced.processRunnerFrequency=250ms",
      "pipelite.advanced.shutdownIfIdle=true"
    })
@ActiveProfiles({"test", "ScheduleRunnerSimpleSshLsfExecutorTest"})
@DirtiesContext
public class ScheduleRunnerSimpleSshLsfExecutorTest {

  @Autowired private ProcessRunnerPoolManager processRunnerPoolManager;
  @Autowired private ServiceConfiguration serviceConfiguration;
  @Autowired private ScheduleService scheduleService;
  @Autowired private ProcessService processService;
  @Autowired private StageService stageService;
  @Autowired private RunnerService runnerService;
  @Autowired private PipeliteMetrics metrics;

  @Autowired
  private List<ConfigurableTestSchedule<SingleStageTestProcessConfiguration>> testSchedules;

  private static final int PROCESS_CNT = 2;
  private static final int IMMEDIATE_RETRIES = 3;
  private static final int MAXIMUM_RETRIES = 3;
  private static final int SCHEDULER_SECONDS = 1;

  @Profile("ScheduleRunnerSimpleSshLsfExecutorTest")
  @TestConfiguration
  static class TestConfig {
    @Autowired private LsfTestConfiguration lsfTestConfiguration;

    @Bean
    public SimpleLsfSchedule simpleLsfSuccessSchedule() {
      return new SimpleLsfSchedule(TestType.SUCCESS, lsfTestConfiguration);
    }

    @Bean
    public SimpleLsfSchedule simpleLsfNonPermanentErrorSchedule() {
      return new SimpleLsfSchedule(TestType.NON_PERMANENT_ERROR, lsfTestConfiguration);
    }

    @Bean
    public SimpleLsfPermanentErrorSchedule simpleLsfPermanentErrorSchedule() {
      return new SimpleLsfPermanentErrorSchedule(lsfTestConfiguration);
    }
  }

  private static int getExitCode(TestType testType) {
    return testType == TestType.NON_PERMANENT_ERROR ? 1 : 0;
  }

  private static class SimpleLsfSchedule
      extends ConfigurableTestSchedule<SingleStageTestProcessConfiguration> {
    public SimpleLsfSchedule(TestType testType, LsfTestConfiguration lsfTestConfiguration) {
      super(
          "0/" + SCHEDULER_SECONDS + " * * * * ?",
          new SingleStageSimpleLsfTestProcessConfiguration(
              testType, IMMEDIATE_RETRIES, MAXIMUM_RETRIES, lsfTestConfiguration));
    }
  }

  private static class SimpleLsfPermanentErrorSchedule
      extends ConfigurableTestSchedule<SingleStageTestProcessConfiguration> {
    public SimpleLsfPermanentErrorSchedule(LsfTestConfiguration lsfTestConfiguration) {
      super(
          "0/" + SCHEDULER_SECONDS + " * * * * ?",
          new SingleStageSimpleLsfTestProcessConfiguration(
              TestType.PERMANENT_ERROR, IMMEDIATE_RETRIES, MAXIMUM_RETRIES, lsfTestConfiguration) {
            @Override
            protected void testExecutorParams(
                SimpleLsfExecutorParameters.SimpleLsfExecutorParametersBuilder<?, ?>
                    executorParamsBuilder) {
              executorParamsBuilder.permanentError(0);
            }
          });
    }
  }

  private void deleteSchedule(
      ConfigurableTestSchedule<SingleStageTestProcessConfiguration> testSchedule) {
    ScheduleEntity schedule = new ScheduleEntity();
    schedule.setPipelineName(testSchedule.pipelineName());
    scheduleService.delete(schedule);
    System.out.println("deleted schedule for pipeline: " + testSchedule.pipelineName());
  }

  private void assertSchedule(ConfigurableTestSchedule<SingleStageTestProcessConfiguration> f) {
    ScheduleRunner scheduleRunner = runnerService.getScheduleRunner();

    assertThat(scheduleRunner.getActiveProcessRunners().size()).isEqualTo(0);
    SingleStageTestProcessConfiguration testProcessConfiguration = f.getRegisteredPipeline();
    assertThat(testProcessConfiguration.configuredProcessIds().size()).isEqualTo(PROCESS_CNT);
    testProcessConfiguration.assertCompletedMetrics(metrics, PROCESS_CNT);
    testProcessConfiguration.assertCompletedScheduleEntity(
        scheduleService, serviceConfiguration.getName(), PROCESS_CNT);
    testProcessConfiguration.assertCompletedProcessEntities(processService, PROCESS_CNT);
    testProcessConfiguration.assertCompletedStageEntities(stageService, PROCESS_CNT);
  }

  @Test
  @EnabledIfEnvironmentVariable(named = "PIPELITE_TEST_LSF_HOST", matches = ".+")
  public void runSchedules() {
    try {
      processRunnerPoolManager.createPools();

      ScheduleRunner scheduleRunner = runnerService.getScheduleRunner();
      for (ConfigurableTestSchedule<SingleStageTestProcessConfiguration> f : testSchedules) {
        scheduleRunner.setMaximumExecutions(f.pipelineName(), PROCESS_CNT);
      }

      processRunnerPoolManager.startPools();
      processRunnerPoolManager.waitPoolsToStop();

      for (ConfigurableTestSchedule<SingleStageTestProcessConfiguration> f : testSchedules) {
        assertSchedule(f);
      }
    } finally {
      for (ConfigurableTestSchedule<SingleStageTestProcessConfiguration> f : testSchedules) {
        deleteSchedule(f);
      }
    }
  }
}
