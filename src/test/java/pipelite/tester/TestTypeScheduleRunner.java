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
package pipelite.tester;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import pipelite.configuration.ServiceConfiguration;
import pipelite.entity.ScheduleEntity;
import pipelite.manager.ProcessRunnerPoolManager;
import pipelite.metrics.PipeliteMetrics;
import pipelite.runner.schedule.ScheduleRunner;
import pipelite.service.*;
import pipelite.tester.pipeline.ConfigurableTestSchedule;
import pipelite.tester.process.SingleStageTestProcessConfiguration;

@Component
public class TestTypeScheduleRunner {

  @Autowired private ServiceConfiguration serviceConfiguration;
  @Autowired private RegisteredPipelineService registeredPipelineService;
  @Autowired private RegisteredScheduleService registeredScheduleService;
  @Autowired private ProcessRunnerPoolManager processRunnerPoolManager;
  @Autowired private RunnerService runnerService;
  @Autowired private ScheduleService scheduleService;
  @Autowired private ProcessService processService;
  @Autowired private PipeliteMetrics metrics;

  public <T extends SingleStageTestProcessConfiguration> void runSchedules(
      StageService stageServiceSpy,
      int schedulerSeconds,
      int processCnt,
      Function<TestType, T> testProcessConfigurationFactory) {

    // Register test pipelines.
    List<ConfigurableTestSchedule<T>> testSchedules = new ArrayList<>();
    for (TestType testType : TestType.init()) {
      ConfigurableTestSchedule<T> schedule =
          new ConfigurableTestSchedule<>(
              "0/" + schedulerSeconds + " * * * * ?",
              testProcessConfigurationFactory.apply(testType));
      testSchedules.add(schedule);
      registeredPipelineService.registerPipeline(schedule);
    }
    registeredScheduleService.saveSchedules();

    // Spy stage service.
    TestType.spyStageService(stageServiceSpy);

    try {
      processRunnerPoolManager.createPools();

      ScheduleRunner scheduleRunner = runnerService.getScheduleRunner();
      for (ConfigurableTestSchedule<T> f : testSchedules) {
        scheduleRunner.setMaximumExecutions(f.pipelineName(), processCnt);
      }

      processRunnerPoolManager.startPools();
      processRunnerPoolManager.waitPoolsToStop();

      for (ConfigurableTestSchedule<T> f : testSchedules) {
        assertSchedule(stageServiceSpy, f, processCnt);
      }
    } finally {
      for (ConfigurableTestSchedule<T> f : testSchedules) {
        deleteSchedule(f);
      }
    }
  }

  private <T extends SingleStageTestProcessConfiguration> void assertSchedule(
      StageService stageServiceSpy, ConfigurableTestSchedule<T> f, int processCnt) {
    ScheduleRunner scheduleRunner = runnerService.getScheduleRunner();

    assertThat(scheduleRunner.getActiveProcessRunners().size()).isEqualTo(0);
    SingleStageTestProcessConfiguration testProcessConfiguration = f.testProcessConfiguration();
    assertThat(f.configuredProcessIds().size()).isEqualTo(processCnt);

    testProcessConfiguration.assertCompleted(processService, stageServiceSpy, metrics, processCnt);
    testProcessConfiguration.assertCompletedScheduleEntity(
        scheduleService, serviceConfiguration.getName(), processCnt);
  }

  private <T extends SingleStageTestProcessConfiguration> void deleteSchedule(
      ConfigurableTestSchedule<T> schedule) {
    ScheduleEntity scheduleEntity = new ScheduleEntity();
    scheduleEntity.setPipelineName(schedule.pipelineName());
    scheduleService.delete(scheduleEntity);
    System.out.println("deleted schedule for pipeline: " + schedule.pipelineName());
  }
}
