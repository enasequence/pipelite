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

import java.time.Duration;
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
import pipelite.PipeliteTestConfigWithManager;
import pipelite.configuration.ServiceConfiguration;
import pipelite.configuration.properties.LsfTestConfiguration;
import pipelite.entity.ProcessEntity;
import pipelite.entity.ScheduleEntity;
import pipelite.entity.StageEntity;
import pipelite.entity.StageLogEntity;
import pipelite.helper.ScheduleTestHelper;
import pipelite.manager.ProcessRunnerPoolManager;
import pipelite.metrics.PipelineMetrics;
import pipelite.metrics.PipeliteMetrics;
import pipelite.metrics.TimeSeriesMetrics;
import pipelite.process.ProcessState;
import pipelite.process.builder.ProcessBuilder;
import pipelite.service.ProcessService;
import pipelite.service.RunnerService;
import pipelite.service.ScheduleService;
import pipelite.service.StageService;
import pipelite.stage.StageState;
import pipelite.stage.parameters.SimpleLsfExecutorParameters;

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

  @Autowired private List<TestSchedule> testSchedules;
  @Autowired private PipeliteMetrics metrics;

  private static final int PROCESS_CNT = 2;

  @Profile("ScheduleRunnerSimpleSshLsfExecutorTest")
  @TestConfiguration
  static class TestConfig {
    @Autowired private LsfTestConfiguration lsfTestConfiguration;

    @Bean
    public TestSchedule schedule() {
      return new TestSchedule(1, lsfTestConfiguration);
    }
  }

  @Getter
  protected static class TestSchedule extends ScheduleTestHelper {
    public final int schedulerSeconds; // 60 must be divisible by schedulerSeconds.
    private final LsfTestConfiguration lsfTestConfiguration;

    public TestSchedule(int schedulerSeconds, LsfTestConfiguration lsfTestConfiguration) {
      super();
      this.schedulerSeconds = schedulerSeconds;
      this.lsfTestConfiguration = lsfTestConfiguration;
    }

    @Override
    protected String _configureCron() {
      return "0/" + schedulerSeconds + " * * * * ?";
    }

    @Override
    public void _configureProcess(ProcessBuilder builder) {
      SimpleLsfExecutorParameters executorParams =
          SimpleLsfExecutorParameters.builder()
              .host(lsfTestConfiguration.getHost())
              .workDir(lsfTestConfiguration.getWorkDir())
              .timeout(Duration.ofSeconds(180))
              .maximumRetries(0)
              .immediateRetries(0)
              .build();
      builder.execute("STAGE").withSimpleLsfExecutor("sleep 10", executorParams);
    }
  }

  private void deleteSchedule(TestSchedule testSchedule) {
    ScheduleEntity schedule = new ScheduleEntity();
    schedule.setPipelineName(testSchedule.pipelineName());
    scheduleService.delete(schedule);
    System.out.println("deleted schedule for pipeline: " + testSchedule.pipelineName());
  }

  private void assertSchedulerMetrics(TestSchedule f) {
    String pipelineName = f.pipelineName();

    PipelineMetrics pipelineMetrics = metrics.pipeline(pipelineName);

    assertThat(pipelineMetrics.process().getCompletedCount()).isEqualTo(PROCESS_CNT);
    assertThat(pipelineMetrics.stage().getFailedCount()).isEqualTo(0L);
    assertThat(pipelineMetrics.stage().getSuccessCount()).isEqualTo(PROCESS_CNT);
    assertThat(TimeSeriesMetrics.getCount(pipelineMetrics.process().getCompletedTimeSeries()))
        .isEqualTo(PROCESS_CNT);
    assertThat(TimeSeriesMetrics.getCount(pipelineMetrics.stage().getFailedTimeSeries()))
        .isEqualTo(0);
    assertThat(TimeSeriesMetrics.getCount(pipelineMetrics.stage().getSuccessTimeSeries()))
        .isEqualTo(PROCESS_CNT);
  }

  private void assertScheduleEntity(List<ScheduleEntity> scheduleEntities, TestSchedule f) {
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
    assertThat(scheduleEntity.getExecutionCount()).isEqualTo(PROCESS_CNT);
    assertThat(scheduleEntity.getCron()).isEqualTo(f.cron());
    assertThat(scheduleEntity.getStartTime()).isNotNull();
    assertThat(scheduleEntity.getEndTime()).isNotNull();
  }

  private void assertProcessEntity(TestSchedule f, String processId) {
    String pipelineName = f.pipelineName();

    ProcessEntity processEntity = processService.getSavedProcess(f.pipelineName(), processId).get();
    assertThat(processEntity.getPipelineName()).isEqualTo(pipelineName);
    assertThat(processEntity.getProcessId()).isEqualTo(processId);
    assertThat(processEntity.getExecutionCount()).isEqualTo(1);
    assertThat(processEntity.getProcessState()).isEqualTo(ProcessState.COMPLETED);
  }

  private void assertStageEntities(TestSchedule f, String processId) {
    String pipelineName = f.pipelineName();

    StageEntity stageEntity =
        stageService.getSavedStage(f.pipelineName(), processId, "STAGE").get();
    StageLogEntity stageLogEntity =
        stageService.getSavedStageLog(f.pipelineName(), processId, "STAGE").get();
    assertThat(stageEntity.getPipelineName()).isEqualTo(pipelineName);
    assertThat(stageEntity.getProcessId()).isEqualTo(processId);
    assertThat(stageEntity.getExecutionCount()).isEqualTo(1);
    assertThat(stageEntity.getStartTime()).isNotNull();
    assertThat(stageEntity.getEndTime()).isNotNull();
    assertThat(stageEntity.getStartTime()).isBeforeOrEqualTo(stageEntity.getEndTime());
    assertThat(stageEntity.getExecutorName()).isEqualTo("pipelite.executor.SimpleLsfExecutor");
    assertThat(stageEntity.getExecutorData()).isEqualTo("{\n" + "  \"cmd\" : \"sleep 10\"\n" + "}");
    assertThat(stageEntity.getExecutorParams())
        .isEqualTo(
            "{\n"
                + "  \"timeout\" : 180000,\n"
                + "  \"maximumRetries\" : 0,\n"
                + "  \"immediateRetries\" : 0,\n"
                + "  \"host\" : \"noah-login\",\n"
                + "  \"workDir\" : \"pipelite-test\",\n"
                + "  \"logBytes\" : 1048576\n"
                + "}");
    assertThat(stageEntity.getStageState()).isEqualTo(StageState.SUCCESS);
    assertThat(stageEntity.getResultParams()).contains("\"exit code\" : \"0\"");
    assertThat(stageEntity.getResultParams()).contains("\"job id\" :");
  }

  private void assertSchedule(TestSchedule f) {
    ScheduleRunner scheduleRunner = runnerService.getScheduleRunner();

    assertThat(scheduleRunner.getActiveProcessRunners().size()).isEqualTo(0);
    List<ScheduleEntity> scheduleEntities =
        scheduleService.getSchedules(serviceConfiguration.getName());
    assertThat(f.configuredProcessIds().size()).isEqualTo(PROCESS_CNT);
    assertSchedulerMetrics(f);
    assertScheduleEntity(scheduleEntities, f);
    for (String processId : f.configuredProcessIds()) {
      assertProcessEntity(f, processId);
      assertStageEntities(f, processId);
    }
  }

  @Test
  public void testSchedules() {
    try {
      processRunnerPoolManager.createPools();

      ScheduleRunner scheduleRunner = runnerService.getScheduleRunner();
      for (TestSchedule f : testSchedules) {
        scheduleRunner.setMaximumExecutions(f.pipelineName(), PROCESS_CNT);
      }

      processRunnerPoolManager.startPools();
      processRunnerPoolManager.waitPoolsToStop();

      for (TestSchedule f : testSchedules) {
        assertSchedule(f);
      }
    } finally {
      for (TestSchedule f : testSchedules) {
        deleteSchedule(f);
      }
    }
  }
}
