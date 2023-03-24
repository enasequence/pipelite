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

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
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
import pipelite.entity.StageEntity;
import pipelite.entity.StageLogEntity;
import pipelite.entity.field.StageState;
import pipelite.manager.ProcessRunnerPoolManager;
import pipelite.metrics.PipeliteMetrics;
import pipelite.metrics.collector.ProcessRunnerMetrics;
import pipelite.process.ProcessState;
import pipelite.process.builder.ProcessBuilder;
import pipelite.service.ProcessService;
import pipelite.service.RunnerService;
import pipelite.service.ScheduleService;
import pipelite.service.StageService;
import pipelite.stage.executor.StageExecutorResult;
import pipelite.stage.parameters.ExecutorParameters;
import pipelite.test.configuration.PipeliteTestConfigWithManager;
import pipelite.tester.pipeline.ConfigurableTestSchedule;

@SpringBootTest(
    classes = PipeliteTestConfigWithManager.class,
    properties = {
      "pipelite.service.force=true",
      "pipelite.service.name=ScheduleRunnerTest",
      "pipelite.advanced.processRunnerFrequency=250ms",
      "pipelite.advanced.shutdownIfIdle=true"
    })
@ActiveProfiles({"pipelite", "ScheduleRunnerTest"})
@DirtiesContext
public class ScheduleRunnerTest {

  @Autowired private ProcessRunnerPoolManager processRunnerPoolManager;
  @Autowired private ServiceConfiguration serviceConfiguration;
  @Autowired private ScheduleService scheduleService;
  @Autowired private ProcessService processService;
  @Autowired private StageService stageService;
  @Autowired private RunnerService runnerService;

  @Autowired private List<TestSchedule> testSchedules;
  @Autowired private PipeliteMetrics metrics;

  @Profile("ScheduleRunnerTest")
  @TestConfiguration
  static class TestConfig {
    @Bean
    public TestSchedule firstScheduleSuccess() {
      return new TestSchedule(2, 2, new TestProcessConfiguration(1, StageTestResult.SUCCESS));
    }

    @Bean
    public TestSchedule secondScheduleSuccess() {
      return new TestSchedule(1, 4, new TestProcessConfiguration(2, StageTestResult.SUCCESS));
    }

    @Bean
    public TestSchedule firstScheduleNonPermanentError() {
      return new TestSchedule(2, 2, new TestProcessConfiguration(1, StageTestResult.ERROR));
    }

    @Bean
    public TestSchedule secondScheduleNonPermanentError() {
      return new TestSchedule(1, 4, new TestProcessConfiguration(2, StageTestResult.ERROR));
    }

    @Bean
    public TestSchedule firstScheduleException() {
      return new TestSchedule(2, 2, new TestProcessConfiguration(1, StageTestResult.EXCEPTION));
    }

    @Bean
    public TestSchedule secondScheduleException() {
      return new TestSchedule(1, 4, new TestProcessConfiguration(2, StageTestResult.EXCEPTION));
    }
  }

  protected enum StageTestResult {
    SUCCESS,
    ERROR,
    EXCEPTION
  }

  @Getter
  protected static class TestProcessConfiguration
      extends pipelite.tester.process.TestProcessConfiguration {
    public final int stageCnt;
    public final StageTestResult stageTestResult;
    public final AtomicLong stageExecCnt = new AtomicLong();

    public TestProcessConfiguration(int stageCnt, StageTestResult stageTestResult) {
      this.stageCnt = stageCnt;
      this.stageTestResult = stageTestResult;
    }

    @Override
    public void configureProcess(ProcessBuilder builder) {
      ExecutorParameters executorParams =
          ExecutorParameters.builder()
              .immediateRetries(0)
              .maximumRetries(0)
              .timeout(Duration.ofSeconds(10))
              .build();
      for (int i = 0; i < stageCnt; ++i) {
        builder
            .execute("STAGE" + i)
            .withSyncTestExecutor(
                (request) -> {
                  stageExecCnt.incrementAndGet();
                  if (stageTestResult == StageTestResult.ERROR) {
                    return StageExecutorResult.executionError();
                  }
                  if (stageTestResult == StageTestResult.SUCCESS) {
                    return StageExecutorResult.success();
                  }
                  if (stageTestResult == StageTestResult.EXCEPTION) {
                    throw new RuntimeException("Expected exception");
                  }
                  throw new RuntimeException("Unexpected exception");
                },
                executorParams);
      }
    }
  }

  @Getter
  protected static class TestSchedule extends ConfigurableTestSchedule<TestProcessConfiguration> {
    public final int processCnt;

    public TestSchedule(
        int processCnt, int schedulerSeconds, TestProcessConfiguration testProcessConfiguration) {
      super("0/" + schedulerSeconds + " * * * * ?", testProcessConfiguration);
      this.processCnt = processCnt;
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

    ProcessRunnerMetrics processRunnerMetrics = metrics.process(pipelineName);

    TestProcessConfiguration t = f.testProcessConfiguration();
    if (t.stageTestResult != StageTestResult.SUCCESS) {
      assertThat(processRunnerMetrics.failedCount()).isEqualTo(t.stageExecCnt.get() / t.stageCnt);
      for (int i = 0; i < t.stageCnt; ++i) {
        assertThat(processRunnerMetrics.stage("STAGE" + i).failedCount()).isEqualTo(f.processCnt);
        assertThat(processRunnerMetrics.stage("STAGE" + i).successCount()).isEqualTo(0L);
      }
    } else {
      assertThat(processRunnerMetrics.completedCount())
          .isEqualTo(t.stageExecCnt.get() / t.stageCnt);
      for (int i = 0; i < t.stageCnt; ++i) {
        assertThat(processRunnerMetrics.stage("STAGE" + i).failedCount()).isEqualTo(0L);
        assertThat(processRunnerMetrics.stage("STAGE" + i).successCount()).isEqualTo(f.processCnt);
      }
    }
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
    assertThat(scheduleEntity.getExecutionCount()).isEqualTo(f.processCnt);
    assertThat(scheduleEntity.getStartTime()).isNotNull();
    assertThat(scheduleEntity.getEndTime()).isNotNull();
  }

  private void assertProcessEntity(TestSchedule f, String processId) {
    String pipelineName = f.pipelineName();

    ProcessEntity processEntity = processService.getSavedProcess(f.pipelineName(), processId).get();
    assertThat(processEntity.getPipelineName()).isEqualTo(pipelineName);
    assertThat(processEntity.getProcessId()).isEqualTo(processId);
    assertThat(processEntity.getExecutionCount()).isEqualTo(1);

    TestProcessConfiguration t = f.testProcessConfiguration();
    if (t.stageTestResult != StageTestResult.SUCCESS) {
      assertThat(processEntity.getProcessState())
          .isEqualTo(ProcessState.FAILED); // no re-executions allowed
    } else {
      assertThat(processEntity.getProcessState()).isEqualTo(ProcessState.COMPLETED);
    }
  }

  private void assertStageEntities(TestSchedule f, String processId) {
    String pipelineName = f.pipelineName();

    TestProcessConfiguration t = f.testProcessConfiguration();
    for (int i = 0; i < t.stageCnt; ++i) {
      StageEntity stageEntity =
          stageService.getSavedStage(f.pipelineName(), processId, "STAGE" + i).get();
      Optional<StageLogEntity> stageLogEntity =
          stageService.getSavedStageLog(f.pipelineName(), processId, "STAGE" + i);
      assertThat(stageEntity.getPipelineName()).isEqualTo(pipelineName);
      assertThat(stageEntity.getProcessId()).isEqualTo(processId);
      assertThat(stageEntity.getExecutionCount()).isEqualTo(1);
      assertThat(stageEntity.getStartTime()).isNotNull();
      assertThat(stageEntity.getEndTime()).isNotNull();
      assertThat(stageEntity.getStartTime()).isBeforeOrEqualTo(stageEntity.getEndTime());
      assertThat(stageEntity.getExecutorName()).isEqualTo("pipelite.executor.SyncTestExecutor");
      assertThat(stageEntity.getExecutorData()).isNull();
      assertThat(stageEntity.getExecutorParams())
          .isEqualTo(
              "{\n"
                  + "  \"timeout\" : 10000,\n"
                  + "  \"maximumRetries\" : 0,\n"
                  + "  \"immediateRetries\" : 0,\n"
                  + "  \"logSave\" : \"ERROR\",\n"
                  + "  \"logLines\" : 1000\n"
                  + "}");

      if (t.stageTestResult == StageTestResult.ERROR) {
        assertThat(stageEntity.getStageState()).isEqualTo(StageState.ERROR);
        assertThat(stageEntity.getResultParams()).isNull();
      } else if (t.stageTestResult == StageTestResult.EXCEPTION) {
        assertThat(stageEntity.getStageState()).isEqualTo(StageState.ERROR);
        assertThat(stageLogEntity.get().getStageLog())
            .contains("java.lang.RuntimeException: Expected exception");
      } else {
        assertThat(stageEntity.getStageState()).isEqualTo(StageState.SUCCESS);
        assertThat(stageEntity.getResultParams()).isNull();
      }
    }
  }

  private void assertSchedule(TestSchedule f) {
    ScheduleRunner scheduleRunner = runnerService.getScheduleRunner();

    assertThat(scheduleRunner.getActiveProcessRunners().size()).isEqualTo(0);
    List<ScheduleEntity> scheduleEntities =
        scheduleService.getSchedules(serviceConfiguration.getName());

    TestProcessConfiguration t = f.testProcessConfiguration();
    assertThat(t.stageExecCnt.get() / t.stageCnt).isEqualTo(f.processCnt);
    assertThat(f.configuredProcessIds().size()).isEqualTo(f.processCnt);
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
        scheduleRunner.setMaximumExecutions(f.pipelineName(), f.processCnt);
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
