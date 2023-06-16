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

import java.util.List;
import lombok.Getter;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Profile;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import pipelite.configuration.ServiceConfiguration;
import pipelite.entity.ProcessEntity;
import pipelite.entity.ScheduleEntity;
import pipelite.manager.ProcessRunnerPoolManager;
import pipelite.metrics.PipeliteMetrics;
import pipelite.metrics.collector.ProcessRunnerMetrics;
import pipelite.process.ProcessState;
import pipelite.process.builder.ProcessBuilder;
import pipelite.service.ProcessService;
import pipelite.service.RunnerService;
import pipelite.service.ScheduleService;
import pipelite.stage.executor.StageExecutorState;
import pipelite.test.configuration.PipeliteTestConfigWithManager;
import pipelite.tester.pipeline.ConfigurableTestSchedule;

@SpringBootTest(
    classes = PipeliteTestConfigWithManager.class,
    properties = {
      "pipelite.service.force=true",
      "pipelite.service.name=ScheduleRunnerFailedToCreateProcessTest",
      "pipelite.advanced.processRunnerFrequency=250ms",
      "pipelite.advanced.shutdownIfIdle=true"
    })
@ActiveProfiles({"pipelite", "ScheduleRunnerFailedToCreateProcessTest"})
@DirtiesContext
public class ScheduleRunnerFailedToCreateProcessTest {

  @Autowired private ProcessRunnerPoolManager processRunnerPoolManager;
  @Autowired private ServiceConfiguration serviceConfiguration;
  @Autowired private ScheduleService scheduleService;
  @Autowired private ProcessService processService;
  @Autowired private RunnerService runnerService;

  @Autowired private List<MissingStageTestSchedule> testSchedules;
  @Autowired private PipeliteMetrics metrics;

  @Profile("ScheduleRunnerFailedToCreateProcessTest")
  @TestConfiguration
  static class TestConfig {
    @Bean
    public MissingStageTestSchedule schedule() {
      return new MissingStageTestSchedule(2, new MissingStageTestProcessConfiguration());
    }
  }

  protected static class MissingStageTestProcessConfiguration
      extends pipelite.tester.process.TestProcessConfiguration {

    @Override
    public void configureProcess(ProcessBuilder builder) {
      builder
          .executeAfter("STAGE", "MISSING_STAGE")
          .withSyncTestExecutor(StageExecutorState.SUCCESS);
    }
  }

  @Getter
  protected static class MissingStageTestSchedule
      extends ConfigurableTestSchedule<MissingStageTestProcessConfiguration> {

    public MissingStageTestSchedule(
        int schedulerSeconds, MissingStageTestProcessConfiguration processConfiguration) {
      super("0/" + schedulerSeconds + " * * * * ?", processConfiguration);
    }
  }

  private void deleteSchedule(MissingStageTestSchedule testSchedule) {
    ScheduleEntity schedule = new ScheduleEntity();
    schedule.setPipelineName(testSchedule.pipelineName());
    scheduleService.delete(schedule);
    System.out.println("deleted schedule for pipeline: " + testSchedule.pipelineName());
  }

  private void assertSchedulerMetrics(MissingStageTestSchedule f) {
    String pipelineName = f.pipelineName();
    ProcessRunnerMetrics processRunnerMetrics = metrics.process(pipelineName);
    assertThat(processRunnerMetrics.failedCount()).isEqualTo(1);
  }

  private void assertScheduleEntity(
      List<ScheduleEntity> scheduleEntities, MissingStageTestSchedule f) {
    String pipelineName = f.pipelineName();

    assertThat(
            scheduleEntities.stream()
                .filter(e -> e.getPipelineName().equals(f.pipelineName()))
                .count())
        .isEqualTo(1);
    ScheduleEntity scheduleEntity =
        scheduleEntities.stream()
            .filter(e -> e.getPipelineName().equals(f.pipelineName()))
            .findFirst()
            .get();
    assertThat(scheduleEntity.getServiceName()).isEqualTo(serviceConfiguration.getName());
    assertThat(scheduleEntity.getPipelineName()).isEqualTo(pipelineName);
    assertThat(scheduleEntity.getProcessId()).isNotNull();
    assertThat(scheduleEntity.getExecutionCount()).isEqualTo(1);
    assertThat(scheduleEntity.getStartTime()).isNotNull();
    assertThat(scheduleEntity.getEndTime()).isNotNull();
    assertThat(scheduleEntity.getStreakFailed()).isEqualTo(1);
    assertThat(scheduleEntity.getStreakCompleted()).isEqualTo(0);
    assertThat(scheduleEntity.isFailed()).isTrue();
  }

  private void assertProcessEntity(MissingStageTestSchedule f, String processId) {
    String pipelineName = f.pipelineName();

    ProcessEntity processEntity = processService.getSavedProcess(f.pipelineName(), processId).get();
    assertThat(processEntity.getPipelineName()).isEqualTo(pipelineName);
    assertThat(processEntity.getProcessId()).isEqualTo(processId);
    assertThat(processEntity.getExecutionCount()).isEqualTo(1);

    MissingStageTestProcessConfiguration t = f.testProcessConfiguration();
    assertThat(processEntity.getProcessState()).isEqualTo(ProcessState.FAILED);
  }

  private void assertSchedule(MissingStageTestSchedule f) {
    ScheduleRunner scheduleRunner = runnerService.getScheduleRunner();

    assertThat(scheduleRunner.getActiveProcessRunners().size()).isEqualTo(0);
    List<ScheduleEntity> scheduleEntities =
        scheduleService.getSchedules(serviceConfiguration.getName());

    MissingStageTestProcessConfiguration t = f.testProcessConfiguration();
    assertSchedulerMetrics(f);
    assertScheduleEntity(scheduleEntities, f);
    for (String processId : f.configuredProcessIds()) {
      assertProcessEntity(f, processId);
    }
  }

  @Test
  public void testSchedule() {
    try {
      processRunnerPoolManager.createPools();

      ScheduleRunner scheduleRunner = runnerService.getScheduleRunner();
      for (MissingStageTestSchedule f : testSchedules) {
        scheduleRunner.setIdleExecutions(f.pipelineName(), 1);
      }

      processRunnerPoolManager.startPools();
      processRunnerPoolManager.waitPoolsToStop();

      for (MissingStageTestSchedule f : testSchedules) {
        assertSchedule(f);
      }
    } finally {
      for (MissingStageTestSchedule f : testSchedules) {
        deleteSchedule(f);
      }
    }
  }
}
