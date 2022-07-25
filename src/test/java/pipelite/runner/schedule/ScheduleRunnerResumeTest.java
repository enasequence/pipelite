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
package pipelite.runner.schedule;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.spy;

import java.time.Duration;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Profile;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import pipelite.PipeliteTestConfigWithManager;
import pipelite.Schedule;
import pipelite.configuration.PipeliteConfiguration;
import pipelite.entity.ProcessEntity;
import pipelite.entity.ScheduleEntity;
import pipelite.metrics.PipeliteMetrics;
import pipelite.service.PipeliteServices;
import pipelite.time.Time;

@SpringBootTest(
    classes = PipeliteTestConfigWithManager.class,
    properties = {
      "pipelite.service.force=true",
      "pipelite.service.name=ScheduleRunnerResumeTest",
      "pipelite.advanced.processRunnerFrequency=250ms",
      "pipelite.advanced.shutdownIfIdle=true"
    })
@ActiveProfiles({"test", "ScheduleRunnerResumeTest"})
@DirtiesContext
public class ScheduleRunnerResumeTest {

  @Autowired private PipeliteConfiguration pipeliteConfiguration;
  @Autowired private PipeliteServices pipeliteServices;
  @Autowired private PipeliteMetrics pipeliteMetrics;

  @Autowired private ScheduleRunnerTest.TestSchedule resume1;
  @Autowired private ScheduleRunnerTest.TestSchedule resume2;

  @Profile("ScheduleRunnerResumeTest")
  @TestConfiguration
  static class TestConfig {
    @Bean
    public ScheduleRunnerTest.TestSchedule resume1() {
      return new ScheduleRunnerTest.TestSchedule(
          1,
          2,
          new ScheduleRunnerTest.TestProcessConfiguration(
              2, ScheduleRunnerTest.StageTestResult.SUCCESS));
    }

    @Bean
    public ScheduleRunnerTest.TestSchedule resume2() {
      return new ScheduleRunnerTest.TestSchedule(
          1,
          1,
          new ScheduleRunnerTest.TestProcessConfiguration(
              2, ScheduleRunnerTest.StageTestResult.SUCCESS));
    }
  }

  @Test
  // @Timeout(value = 10, unit = TimeUnit.SECONDS)
  public void testResumeSchedules() {
    int maxExecution1 = 1;
    int maxExecution2 = 1;

    // Create two schedules with start time and process id to allow processes to resume.

    ZonedDateTime startTime1 = ZonedDateTime.now().minusHours(2).truncatedTo(ChronoUnit.SECONDS);
    ZonedDateTime startTime2 = ZonedDateTime.now().minusHours(2).truncatedTo(ChronoUnit.SECONDS);
    ZonedDateTime nextTime1 = ZonedDateTime.now().minusMinutes(1).truncatedTo(ChronoUnit.SECONDS);
    ZonedDateTime nextTime2 = ZonedDateTime.now().minusHours(1).truncatedTo(ChronoUnit.SECONDS);

    ScheduleEntity scheduleEntity1 =
        pipeliteServices.schedule().getSavedSchedule(resume1.pipelineName()).get();
    ScheduleEntity scheduleEntity2 =
        pipeliteServices.schedule().getSavedSchedule(resume2.pipelineName()).get();
    scheduleEntity1.setStartTime(startTime1);
    scheduleEntity2.setStartTime(startTime2);
    scheduleEntity1.setNextTime(nextTime1);
    scheduleEntity2.setNextTime(nextTime2);
    String processId1 = "1";
    String processId2 = "2";
    scheduleEntity1.setProcessId(processId1);
    scheduleEntity2.setProcessId(processId2);
    pipeliteServices.schedule().saveSchedule(scheduleEntity1);
    pipeliteServices.schedule().saveSchedule(scheduleEntity2);

    ProcessEntity processEntity1 =
        ProcessEntity.createExecution(resume1.pipelineName(), processId1, 5);
    ProcessEntity processEntity2 =
        ProcessEntity.createExecution(resume2.pipelineName(), processId2, 5);
    pipeliteServices.process().saveProcess(processEntity1);
    pipeliteServices.process().saveProcess(processEntity2);

    ScheduleRunner scheduleRunner =
        spy(
            ScheduleRunnerFactory.create(
                pipeliteConfiguration,
                pipeliteServices,
                pipeliteMetrics,
                pipeliteServices.registeredPipeline().getRegisteredPipelines(Schedule.class)));

    scheduleRunner.setMaximumExecutions(resume1.pipelineName(), maxExecution1);
    scheduleRunner.setMaximumExecutions(resume2.pipelineName(), maxExecution2);

    // Resume the two processes, check that they are immediately executed
    // and that they are scheduled for a later execution.

    scheduleRunner.startUp();

    while (!scheduleRunner.isIdle()) {
      scheduleRunner.runOneIteration();
      Time.wait(Duration.ofMillis(100));
    }

    scheduleEntity1 = pipeliteServices.schedule().getSavedSchedule(resume1.pipelineName()).get();
    scheduleEntity2 = pipeliteServices.schedule().getSavedSchedule(resume2.pipelineName()).get();
    assertThat(scheduleEntity1.getStartTime()).isEqualTo(startTime1);
    assertThat(scheduleEntity2.getStartTime()).isEqualTo(startTime2);
    assertThat(scheduleEntity1.getEndTime()).isAfter(startTime1);
    assertThat(scheduleEntity2.getEndTime()).isAfter(startTime2);
    assertThat(scheduleEntity1.getExecutionCount()).isOne();
    assertThat(scheduleEntity2.getExecutionCount()).isOne();
    assertThat(scheduleEntity1.getProcessId()).isEqualTo(processId1);
    assertThat(scheduleEntity2.getProcessId()).isEqualTo(processId2);
  }
}
