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

import com.google.common.collect.Iterables;
import java.util.List;
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
import pipelite.helper.SingleStageSimpleLsfScheduleTestHelper;
import pipelite.manager.ProcessRunnerPoolManager;
import pipelite.metrics.PipelineMetrics;
import pipelite.metrics.PipeliteMetrics;
import pipelite.metrics.TimeSeriesMetrics;
import pipelite.process.ProcessState;
import pipelite.service.ProcessService;
import pipelite.service.RunnerService;
import pipelite.service.ScheduleService;
import pipelite.service.StageService;
import pipelite.stage.StageState;
import pipelite.stage.executor.ErrorType;
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
  @Autowired private PipeliteMetrics metrics;

  @Autowired private List<TestSchedule> testSchedules;

  private static final int PROCESS_CNT = 2;
  private static final int RETRY_CNT = 3;
  private static final int SCHEDULER_SECONDS = 1;

  @Profile("ScheduleRunnerSimpleSshLsfExecutorTest")
  @TestConfiguration
  static class TestConfig {
    @Autowired private LsfTestConfiguration lsfTestConfiguration;

    @Bean
    public SuccessSchedule successSchedule() {
      return new SuccessSchedule(lsfTestConfiguration);
    }

    @Bean
    public NonPermanentErrorSchedule nonPermanentErrorSchedule() {
      return new NonPermanentErrorSchedule(lsfTestConfiguration);
    }

    @Bean
    public PermanentErrorSchedule permanentErrorSchedule() {
      return new PermanentErrorSchedule(lsfTestConfiguration);
    }
  }

  protected static class TestSchedule extends SingleStageSimpleLsfScheduleTestHelper {
    public TestSchedule(int exitCode, LsfTestConfiguration lsfTestConfiguration) {
      super(
          "0/" + SCHEDULER_SECONDS + " * * * * ?",
          exitCode,
          RETRY_CNT,
          RETRY_CNT,
          lsfTestConfiguration);
    }
  }

  protected static class SuccessSchedule extends TestSchedule {
    public SuccessSchedule(LsfTestConfiguration lsfTestConfiguration) {
      super(0, lsfTestConfiguration);
    }
  }

  protected static class NonPermanentErrorSchedule extends TestSchedule {
    public NonPermanentErrorSchedule(LsfTestConfiguration lsfTestConfiguration) {
      super(1, lsfTestConfiguration);
    }
  }

  protected static class PermanentErrorSchedule extends TestSchedule {
    public PermanentErrorSchedule(LsfTestConfiguration lsfTestConfiguration) {
      super(0, lsfTestConfiguration);
    }

    @Override
    protected void testExecutorParams(
        SimpleLsfExecutorParameters.SimpleLsfExecutorParametersBuilder<?, ?>
            executorParamsBuilder) {
      executorParamsBuilder.permanentError(0);
    }
  }

  private boolean isExpectedError(TestSchedule testSchedule) {
    return isExpectedPermanentError(testSchedule) || isExpectedNonPermanentError(testSchedule);
  }

  private boolean isExpectedPermanentError(TestSchedule testSchedule) {
    return testSchedule instanceof PermanentErrorSchedule;
  }

  private boolean isExpectedNonPermanentError(TestSchedule testSchedule) {
    return testSchedule instanceof NonPermanentErrorSchedule;
  }

  private void deleteSchedule(TestSchedule testSchedule) {
    ScheduleEntity schedule = new ScheduleEntity();
    schedule.setPipelineName(testSchedule.pipelineName());
    scheduleService.delete(schedule);
    System.out.println("deleted schedule for pipeline: " + testSchedule.pipelineName());
  }

  private void assertMetrics(TestSchedule f) {
    String pipelineName = f.pipelineName();

    PipelineMetrics pipelineMetrics = metrics.pipeline(pipelineName);

    if (isExpectedPermanentError(f)) {
      assertThat(pipelineMetrics.process().getCompletedCount()).isEqualTo(0L);
      assertThat(pipelineMetrics.stage().getFailedCount()).isEqualTo(PROCESS_CNT);
      assertThat(pipelineMetrics.stage().getSuccessCount()).isEqualTo(0L);
      assertThat(TimeSeriesMetrics.getCount(pipelineMetrics.process().getFailedTimeSeries()))
          .isEqualTo(PROCESS_CNT);
      assertThat(TimeSeriesMetrics.getCount(pipelineMetrics.process().getCompletedTimeSeries()))
          .isEqualTo(0);
      assertThat(TimeSeriesMetrics.getCount(pipelineMetrics.stage().getFailedTimeSeries()))
          .isEqualTo(PROCESS_CNT);
      assertThat(TimeSeriesMetrics.getCount(pipelineMetrics.stage().getSuccessTimeSeries()))
          .isEqualTo(0);
    } else if (isExpectedNonPermanentError(f)) {
      assertThat(pipelineMetrics.process().getCompletedCount()).isEqualTo(0L);
      assertThat(pipelineMetrics.stage().getFailedCount()).isEqualTo(PROCESS_CNT * (1 + RETRY_CNT));
      assertThat(pipelineMetrics.stage().getSuccessCount()).isEqualTo(0L);
      assertThat(TimeSeriesMetrics.getCount(pipelineMetrics.process().getFailedTimeSeries()))
          .isEqualTo(PROCESS_CNT);
      assertThat(TimeSeriesMetrics.getCount(pipelineMetrics.process().getCompletedTimeSeries()))
          .isEqualTo(0);
      assertThat(TimeSeriesMetrics.getCount(pipelineMetrics.stage().getFailedTimeSeries()))
          .isEqualTo(PROCESS_CNT * (1 + RETRY_CNT));
      assertThat(TimeSeriesMetrics.getCount(pipelineMetrics.stage().getSuccessTimeSeries()))
          .isEqualTo(0);
    } else {
      assertThat(pipelineMetrics.process().getCompletedCount()).isEqualTo(PROCESS_CNT);
      assertThat(pipelineMetrics.stage().getFailedCount()).isEqualTo(0L);
      assertThat(pipelineMetrics.stage().getSuccessCount()).isEqualTo(PROCESS_CNT);
      assertThat(TimeSeriesMetrics.getCount(pipelineMetrics.process().getFailedTimeSeries()))
          .isEqualTo(0);
      assertThat(TimeSeriesMetrics.getCount(pipelineMetrics.process().getCompletedTimeSeries()))
          .isEqualTo(PROCESS_CNT);
      assertThat(TimeSeriesMetrics.getCount(pipelineMetrics.stage().getFailedTimeSeries()))
          .isEqualTo(0);
      assertThat(TimeSeriesMetrics.getCount(pipelineMetrics.stage().getSuccessTimeSeries()))
          .isEqualTo(PROCESS_CNT);
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
    assertThat(scheduleEntity.getProcessId())
        .isEqualTo(Iterables.getLast(f.configuredProcessIds()));
    assertThat(scheduleEntity.getExecutionCount()).isEqualTo(PROCESS_CNT);
    assertThat(scheduleEntity.getStartTime()).isNotNull();
    assertThat(scheduleEntity.getEndTime()).isAfter(scheduleEntity.getStartTime());
    if (isExpectedError(f)) {
      assertThat(scheduleEntity.getLastFailed()).isAfter(scheduleEntity.getStartTime());
      assertThat(scheduleEntity.getLastCompleted()).isNull();
      assertThat(scheduleEntity.getStreakFailed()).isEqualTo(PROCESS_CNT);
      assertThat(scheduleEntity.getStreakCompleted()).isEqualTo(0);
    } else {
      assertThat(scheduleEntity.getLastFailed()).isNull();
      assertThat(scheduleEntity.getLastCompleted()).isAfter(scheduleEntity.getStartTime());
      assertThat(scheduleEntity.getStreakFailed()).isEqualTo(0);
      assertThat(scheduleEntity.getStreakCompleted()).isEqualTo(PROCESS_CNT);
    }
  }

  private void assertProcessEntity(TestSchedule f, String processId) {
    String pipelineName = f.pipelineName();

    ProcessEntity processEntity = processService.getSavedProcess(f.pipelineName(), processId).get();
    assertThat(processEntity.getPipelineName()).isEqualTo(pipelineName);
    assertThat(processEntity.getProcessId()).isEqualTo(processId);
    assertThat(processEntity.getExecutionCount()).isEqualTo(1);
    if (!isExpectedError(f)) {
      assertThat(processEntity.getProcessState()).isEqualTo(ProcessState.COMPLETED);
    } else {
      assertThat(processEntity.getProcessState()).isEqualTo(ProcessState.FAILED);
    }
    assertThat(processEntity.getStartTime()).isNotNull();
    assertThat(processEntity.getEndTime()).isAfter(processEntity.getStartTime());
  }

  private void assertStageEntity(TestSchedule f, String processId) {
    String pipelineName = f.pipelineName();

    StageEntity stageEntity =
        stageService.getSavedStage(f.pipelineName(), processId, f.stageName()).get();
    assertThat(stageEntity.getPipelineName()).isEqualTo(pipelineName);
    assertThat(stageEntity.getProcessId()).isEqualTo(processId);
    if (isExpectedNonPermanentError(f)) {
      assertThat(stageEntity.getExecutionCount()).isEqualTo(RETRY_CNT + 1);
    } else {
      assertThat(stageEntity.getExecutionCount()).isEqualTo(1);
    }
    assertThat(stageEntity.getStartTime()).isNotNull();
    assertThat(stageEntity.getEndTime()).isAfter(stageEntity.getStartTime());
    assertThat(stageEntity.getExecutorName()).isEqualTo("pipelite.executor.SimpleLsfExecutor");
    assertThat(stageEntity.getExecutorData()).contains("  \"cmd\" : \"" + f.cmd() + "\"");
    assertThat(stageEntity.getExecutorData()).contains("  \"jobId\" : \"");
    assertThat(stageEntity.getExecutorData()).contains("  \"outFile\" : \"");
    assertThat(stageEntity.getExecutorParams())
        .isEqualTo(
            "{\n"
                + "  \"timeout\" : 180000,\n"
                + "  \"maximumRetries\" : "
                + RETRY_CNT
                + ",\n"
                + "  \"immediateRetries\" : "
                + RETRY_CNT
                + ",\n"
                + "  \"host\" : \"noah-login\",\n"
                + "  \"workDir\" : \"pipelite-test\",\n"
                + (isExpectedPermanentError(f)
                    ? "  \"logBytes\" : 1048576,\n"
                        + "  \"permanentErrors\" : [ "
                        + f.exitCode()
                        + " ]\n"
                    : "  \"logBytes\" : 1048576\n")
                + "}");
    if (isExpectedError(f)) {
      assertThat(stageEntity.getStageState()).isEqualTo(StageState.ERROR);
    } else {
      assertThat(stageEntity.getStageState()).isEqualTo(StageState.SUCCESS);
    }
    if (isExpectedPermanentError(f)) {
      assertThat(stageEntity.getErrorType()).isEqualTo(ErrorType.PERMANENT_ERROR);
    } else if (isExpectedNonPermanentError(f)) {
      assertThat(stageEntity.getErrorType()).isEqualTo(ErrorType.EXECUTION_ERROR);
    } else {
      assertThat(stageEntity.getErrorType()).isNull();
    }
    assertThat(stageEntity.getResultParams()).contains("\"exit code\" : \"" + f.exitCode() + "\"");
    assertThat(stageEntity.getResultParams()).contains("\"job id\" :");
  }

  private void assertSchedule(TestSchedule f) {
    ScheduleRunner scheduleRunner = runnerService.getScheduleRunner();

    assertThat(scheduleRunner.getActiveProcessRunners().size()).isEqualTo(0);
    assertThat(f.configuredProcessIds().size()).isEqualTo(PROCESS_CNT);
    assertMetrics(f);
    List<ScheduleEntity> scheduleEntities =
        scheduleService.getSchedules(serviceConfiguration.getName());
    assertScheduleEntity(scheduleEntities, f);
    for (String processId : f.configuredProcessIds()) {
      assertProcessEntity(f, processId);
      assertStageEntity(f, processId);
    }
  }

  @Test
  public void runSchedules() {
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
