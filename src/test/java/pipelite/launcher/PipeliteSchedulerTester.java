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
package pipelite.launcher;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.Data;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;
import pipelite.UniqueStringGenerator;
import pipelite.configuration.LauncherConfiguration;
import pipelite.entity.ScheduleEntity;
import pipelite.process.Process;
import pipelite.process.ProcessFactory;
import pipelite.process.builder.ProcessBuilder;
import pipelite.service.ScheduleService;
import pipelite.stage.StageExecutionResult;

@Component
@Scope("prototype")
public class PipeliteSchedulerTester {

  private final PipeliteScheduler pipeliteScheduler;
  private final ScheduleService scheduleService;
  private final LauncherConfiguration launcherConfiguration;

  public PipeliteSchedulerTester(
      @Autowired PipeliteScheduler pipeliteScheduler,
      @Autowired ScheduleService scheduleService,
      @Autowired LauncherConfiguration launcherConfiguration) {
    this.pipeliteScheduler = pipeliteScheduler;
    this.scheduleService = scheduleService;
    this.launcherConfiguration = launcherConfiguration;
  }

  @TestConfiguration
  static class TestConfig {
    @Bean
    public ProcessFactory oneProcess() {
      return new TestProcessFactory(ONE);
    }

    @Bean
    public ProcessFactory threeProcesses1() {
      return new TestProcessFactory(THREE_1);
    }

    @Bean
    public ProcessFactory threeProcesses2() {
      return new TestProcessFactory(THREE_2);
    }

    @Bean
    public ProcessFactory threeProcesses3() {
      return new TestProcessFactory(THREE_3);
    }

    @Bean
    public ProcessFactory oneProcessOneFailure() {
      return new TestProcessFactory(ONE_FAILURE);
    }

    @Bean
    public ProcessFactory threeProcessesOneFailure1() {
      return new TestProcessFactory(THREE_ONE_FAILURE_1);
    }

    @Bean
    public ProcessFactory threeProcessesOneFailure2() {
      return new TestProcessFactory(THREE_ONE_FAILURE_2);
    }

    @Bean
    public ProcessFactory threeProcessesOneFailure3() {
      return new TestProcessFactory(THREE_ONE_FAILURE_3);
    }

    @Bean
    public ProcessFactory threeProcessesAllFailure1() {
      return new TestProcessFactory(THREE_ALL_FAILURE_1);
    }

    @Bean
    public ProcessFactory threeProcessesAllFailure2() {
      return new TestProcessFactory(THREE_ALL_FAILURE_2);
    }

    @Bean
    public ProcessFactory threeProcessesAllFailure3() {
      return new TestProcessFactory(THREE_ALL_FAILURE_3);
    }
  }

  private static final Duration STOP_AFTER = Duration.ofSeconds(10);

  private static final ScheduleTest ONE = new ScheduleTest(2, false);
  private static final ScheduleTest THREE_1 = new ScheduleTest(2, false);
  private static final ScheduleTest THREE_2 = new ScheduleTest(4, false);
  private static final ScheduleTest THREE_3 = new ScheduleTest(6, false);
  private static final ScheduleTest ONE_FAILURE = new ScheduleTest(6, true);
  private static final ScheduleTest THREE_ONE_FAILURE_1 = new ScheduleTest(2, false);
  private static final ScheduleTest THREE_ONE_FAILURE_2 = new ScheduleTest(4, false);
  private static final ScheduleTest THREE_ONE_FAILURE_3 = new ScheduleTest(6, true);
  private static final ScheduleTest THREE_ALL_FAILURE_1 = new ScheduleTest(2, true);
  private static final ScheduleTest THREE_ALL_FAILURE_2 = new ScheduleTest(4, true);
  private static final ScheduleTest THREE_ALL_FAILURE_3 = new ScheduleTest(6, true);

  @Data
  private static class ScheduleTest {
    private final int seconds; // 60 must be divisible by seconds.
    private final boolean failure;
    private final String pipelineName = UniqueStringGenerator.randomPipelineName();
    public final AtomicInteger processExecCnt = new AtomicInteger();
    public final AtomicInteger stageExecCnt = new AtomicInteger();

    public void reset() {
      processExecCnt.set(0);
      stageExecCnt.set(0);
    }
  }

  public static class TestProcessFactory implements ProcessFactory {
    private final ScheduleTest scheduleTest;

    public TestProcessFactory(ScheduleTest scheduleTest) {
      this.scheduleTest = scheduleTest;
    }

    @Override
    public String getPipelineName() {
      return scheduleTest.pipelineName;
    }

    @Override
    public Process create(String processId) {
      return new ProcessBuilder(getPipelineName(), processId)
          .execute("STAGE1")
          .with(
              (stage) -> {
                scheduleTest.processExecCnt.incrementAndGet();
                scheduleTest.stageExecCnt.incrementAndGet();
                if (scheduleTest.failure) {
                  return StageExecutionResult.error();
                } else {
                  return StageExecutionResult.success();
                }
              })
          .execute("STAGE2")
          .with(
              (stage) -> {
                scheduleTest.stageExecCnt.incrementAndGet();
                if (scheduleTest.failure) {
                  return StageExecutionResult.error();
                } else {
                  return StageExecutionResult.success();
                }
              })
          .build();
    }
  }

  private void saveSchedule(ScheduleTest scheduleTest) {
    ScheduleEntity schedule = new ScheduleEntity();
    schedule.setSchedule("0/" + scheduleTest.seconds + " * * * * ?");
    schedule.setLauncherName(launcherConfiguration.getLauncherName());
    schedule.setPipelineName(scheduleTest.pipelineName);
    scheduleService.saveProcessSchedule(schedule);
    System.out.println(
        "saved schedule for pipeline: "
            + scheduleTest.pipelineName
            + ", launcher: "
            + launcherConfiguration.getLauncherName());
  }

  private void deleteSchedule(ScheduleTest scheduleTest) {
    ScheduleEntity schedule = new ScheduleEntity();
    schedule.setPipelineName(scheduleTest.pipelineName);
    scheduleService.delete(schedule);
    System.out.println(
        "deleted schedule for pipeline: "
            + scheduleTest.pipelineName
            + ", launcher: "
            + launcherConfiguration.getLauncherName());
  }

  public void assertResult(PipeliteScheduler pipeliteScheduler, List<ScheduleTest> results) {

    int totalProcessCompletedCount = 0;
    int totalStageFailedCount = 0;
    int totalStageCompletedCount = 0;

    for (ScheduleTest result : results) {

      totalProcessCompletedCount += result.processExecCnt.get();
      ;
      if (result.failure) {
        totalStageFailedCount += result.stageExecCnt.get();
      } else {
        totalStageCompletedCount += result.stageExecCnt.get();
      }

      // Minimum delay before first process is executed is ~0.
      // Maximum executed processes is estimated as: STOP_AFTER / scheduleSeconds[i] + 1
      // Maximum delay before first process is executed is ~scheduleSeconds[i].
      // processLaunchFrequency may prevent last scheduled process execution.
      // Minimum executed processes is estimated as STOP_AFTER / scheduleSeconds[i] - 2

      int expectedProcessExecCnt = (int) STOP_AFTER.toMillis() / 1000 / result.seconds;
      int minExpectedProcessExecCnt = Math.max(0, expectedProcessExecCnt - 2);
      int maxExpectedProcessExecCnt = expectedProcessExecCnt + 1;
      int minExpectedStageExecCnt = minExpectedProcessExecCnt * 2;
      int maxExpectedStageExecCnt = maxExpectedProcessExecCnt * 2;

      assertThat(result.processExecCnt.get())
          .isBetween(minExpectedProcessExecCnt, maxExpectedProcessExecCnt);
      assertThat(result.stageExecCnt.get())
          .isBetween(minExpectedStageExecCnt, maxExpectedStageExecCnt);
    }

    assertThat(pipeliteScheduler.getActiveProcessCount()).isEqualTo(0);
    assertThat(pipeliteScheduler.getProcessFailedToCreateCount()).isEqualTo(0);
    assertThat(pipeliteScheduler.getProcessExceptionCount()).isEqualTo(0);
    assertThat(pipeliteScheduler.getProcessCompletedCount()).isEqualTo(totalProcessCompletedCount);
    assertThat(pipeliteScheduler.getStageFailedCount()).isEqualTo(totalStageFailedCount);
    assertThat(pipeliteScheduler.getStageCompletedCount()).isEqualTo(totalStageCompletedCount);
  }

  public void testOneProcess() {
    ONE.reset();

    try {
      saveSchedule(ONE);

      pipeliteScheduler.setShutdownAfter(STOP_AFTER);

      ServerManager.run(pipeliteScheduler, pipeliteScheduler.serviceName());

      assertResult(pipeliteScheduler, Arrays.asList(ONE));
    } finally {
      deleteSchedule(ONE);
    }
  }

  public void testThreeProcesses() {
    THREE_1.reset();
    THREE_2.reset();
    THREE_3.reset();

    try {
      saveSchedule(THREE_1);
      saveSchedule(THREE_2);
      saveSchedule(THREE_3);

      pipeliteScheduler.setShutdownAfter(STOP_AFTER);

      ServerManager.run(pipeliteScheduler, pipeliteScheduler.serviceName());

      assertResult(pipeliteScheduler, Arrays.asList(THREE_1, THREE_2, THREE_3));

    } finally {
      deleteSchedule(THREE_1);
      deleteSchedule(THREE_2);
      deleteSchedule(THREE_3);
    }
  }

  public void testOneProcessesOneFailure() {
    ONE_FAILURE.reset();

    try {
      saveSchedule(ONE_FAILURE);

      pipeliteScheduler.setShutdownAfter(STOP_AFTER);

      ServerManager.run(pipeliteScheduler, pipeliteScheduler.serviceName());

      assertResult(pipeliteScheduler, Arrays.asList(ONE_FAILURE));

    } finally {
      deleteSchedule(ONE_FAILURE);
    }
  }

  public void testThreeProcessesOneFailure() {
    THREE_ONE_FAILURE_1.reset();
    THREE_ONE_FAILURE_2.reset();
    THREE_ONE_FAILURE_3.reset();

    try {
      saveSchedule(THREE_ONE_FAILURE_1);
      saveSchedule(THREE_ONE_FAILURE_2);
      saveSchedule(THREE_ONE_FAILURE_3);

      pipeliteScheduler.setShutdownAfter(STOP_AFTER);

      ServerManager.run(pipeliteScheduler, pipeliteScheduler.serviceName());

      assertResult(
          pipeliteScheduler,
          Arrays.asList(THREE_ONE_FAILURE_1, THREE_ONE_FAILURE_2, THREE_ONE_FAILURE_3));

    } finally {
      deleteSchedule(THREE_ONE_FAILURE_1);
      deleteSchedule(THREE_ONE_FAILURE_2);
      deleteSchedule(THREE_ONE_FAILURE_3);
    }
  }

  public void testThreeProcessesAllFailure() {
    THREE_ALL_FAILURE_1.reset();
    THREE_ALL_FAILURE_2.reset();
    THREE_ALL_FAILURE_3.reset();

    try {
      saveSchedule(THREE_ALL_FAILURE_1);
      saveSchedule(THREE_ALL_FAILURE_2);
      saveSchedule(THREE_ALL_FAILURE_3);

      pipeliteScheduler.setShutdownAfter(STOP_AFTER);

      ServerManager.run(pipeliteScheduler, pipeliteScheduler.serviceName());

      assertResult(
          pipeliteScheduler,
          Arrays.asList(THREE_ALL_FAILURE_1, THREE_ALL_FAILURE_2, THREE_ALL_FAILURE_3));

    } finally {
      deleteSchedule(THREE_ALL_FAILURE_1);
      deleteSchedule(THREE_ALL_FAILURE_2);
      deleteSchedule(THREE_ALL_FAILURE_3);
    }
  }
}
