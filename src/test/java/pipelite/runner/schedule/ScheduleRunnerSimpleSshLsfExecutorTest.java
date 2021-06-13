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
import pipelite.entity.ScheduleEntity;
import pipelite.helper.*;
import pipelite.manager.ProcessRunnerPoolManager;
import pipelite.metrics.PipeliteMetrics;
import pipelite.service.ProcessService;
import pipelite.service.RunnerService;
import pipelite.service.ScheduleService;
import pipelite.service.StageService;
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
  private static final int IMMEDIATE_RETRIES = 3;
  private static final int MAXIMUM_RETRIES = 3;
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

  protected static class TestSchedule extends SingleStageSimpleLsfTestSchedule {
    public TestSchedule(int exitCode, LsfTestConfiguration lsfTestConfiguration) {
      super(
          "0/" + SCHEDULER_SECONDS + " * * * * ?",
          exitCode,
          IMMEDIATE_RETRIES,
          MAXIMUM_RETRIES,
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

  private TestType getTestType(TestSchedule testSchedule) {
    if (testSchedule instanceof PermanentErrorSchedule) {
      return TestType.PERMANENT_ERROR;
    }
    if (testSchedule instanceof NonPermanentErrorSchedule) {
      return TestType.NON_PERMANENT_ERROR;
    }
    return TestType.SUCCESS;
  }

  private void deleteSchedule(TestSchedule testSchedule) {
    ScheduleEntity schedule = new ScheduleEntity();
    schedule.setPipelineName(testSchedule.pipelineName());
    scheduleService.delete(schedule);
    System.out.println("deleted schedule for pipeline: " + testSchedule.pipelineName());
  }

  private void assertMetrics(TestSchedule f) {
    MetricsTestHelper.assertCompletedMetrics(
        getTestType(f), metrics, f.pipelineName(), PROCESS_CNT, IMMEDIATE_RETRIES, MAXIMUM_RETRIES);
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
    if (getTestType(f) == TestType.NON_PERMANENT_ERROR
        || getTestType(f) == TestType.PERMANENT_ERROR) {
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
    ProcessEntityTestHelper.assertProcessEntity(
        processService, f.pipelineName(), processId, getTestType(f));
  }

  private void assertStageEntity(TestSchedule f, String processId) {
    StageEntityTestHelper.assertCompletedSimpleLsfExecutorStageEntity(
        getTestType(f),
        stageService,
        f.pipelineName(),
        processId,
        f.stageName(),
        f.executorParams().getPermanentErrors(),
        f.cmd(),
        f.exitCode(),
        IMMEDIATE_RETRIES,
        MAXIMUM_RETRIES);
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
