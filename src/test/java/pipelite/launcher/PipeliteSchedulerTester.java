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

import lombok.Data;
import org.springframework.beans.factory.annotation.Autowired;
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

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

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

  private static final Duration STOP_AFTER = Duration.ofSeconds(10);

  @Data
  private static class ExecCnt {
    public final AtomicInteger processExecutionCount = new AtomicInteger();
    public final AtomicInteger stageExecutionCount = new AtomicInteger();

    public void reset() {
      processExecutionCount.set(0);
      stageExecutionCount.set(0);
    }
  }

  public static class TestProcessFactory implements ProcessFactory {
    public final String pipelineName;
    public final ExecCnt execCnt;
    public final boolean error;

    public TestProcessFactory(String pipelineName, ExecCnt execCnt, boolean error) {
      this.pipelineName = pipelineName;
      this.execCnt = execCnt;
      this.error = error;
    }

    @Override
    public String getPipelineName() {
      return pipelineName;
    }

    @Override
    public Process create(String processId) {
      return new ProcessBuilder(pipelineName, processId, 9)
          .execute("STAGE1")
          .with(
              (stage) -> {
                System.out.println(
                    "executing pipeline: "
                        + pipelineName
                        + " process: "
                        + processId
                        + " stage: "
                        + stage.getStageName());
                execCnt.processExecutionCount.incrementAndGet();
                execCnt.stageExecutionCount.incrementAndGet();
                if (error) {
                  return StageExecutionResult.error();
                } else {
                  return StageExecutionResult.success();
                }
              })
          .execute("STAGE2")
          .with(
              (stage) -> {
                System.out.println(
                    "executing pipeline: "
                        + pipelineName
                        + " process: "
                        + processId
                        + " stage: "
                        + stage.getStageName());
                execCnt.stageExecutionCount.incrementAndGet();
                if (error) {
                  return StageExecutionResult.error();
                } else {
                  return StageExecutionResult.success();
                }
              })
          .build();
    }
  }

  @Data
  private static class TestResult {
    private final int scheduleSeconds;
    private final ExecCnt execCnt;
    private final boolean failure;
  }

  private static final String PIPELINE_NAME_ONE_PROCESS =
      UniqueStringGenerator.randomPipelineName();
  private static final String PIPELINE_NAME_THREE_PROCESS_1 =
      UniqueStringGenerator.randomPipelineName();
  private static final String PIPELINE_NAME_THREE_PROCESS_2 =
      UniqueStringGenerator.randomPipelineName();
  private static final String PIPELINE_NAME_THREE_PROCESS_3 =
      UniqueStringGenerator.randomPipelineName();
  private static final String PIPELINE_NAME_ONE_PROCESS_ONE_FAILURE =
      UniqueStringGenerator.randomPipelineName();
  private static final String PIPELINE_NAME_THREE_PROCESS_ONE_FAILURE_1 =
      UniqueStringGenerator.randomPipelineName();
  private static final String PIPELINE_NAME_THREE_PROCESS_ONE_FAILURE_2 =
      UniqueStringGenerator.randomPipelineName();
  private static final String PIPELINE_NAME_THREE_PROCESS_ONE_FAILURE_3 =
      UniqueStringGenerator.randomPipelineName();

  // 60 must be divisible by SCHEDULE_SECONDS.
  private static final int SCHEDULE_SECONDS_ONE_PROCESS = 2;
  private static final int SCHEDULE_SECONDS_THREE_PROCESS_1 = 2;
  private static final int SCHEDULE_SECONDS_THREE_PROCESS_2 = 4;
  private static final int SCHEDULE_SECONDS_THREE_PROCESS_3 = 6;
  private static final int SCHEDULE_SECONDS_ONE_PROCESS_ONE_FAILURE = 6;
  private static final int SCHEDULE_SECONDS_THREE_PROCESS_ONE_FAILURE_1 = 2;
  private static final int SCHEDULE_SECONDS_THREE_PROCESS_ONE_FAILURE_2 = 4;
  private static final int SCHEDULE_SECONDS_THREE_PROCESS_ONE_FAILURE_3 = 6;

  private static final ExecCnt EXEC_CNT_ONE_PROCESS = new ExecCnt();
  private static final ExecCnt EXEC_CNT_THREE_PROCESS_1 = new ExecCnt();
  private static final ExecCnt EXEC_CNT_THREE_PROCESS_2 = new ExecCnt();
  private static final ExecCnt EXEC_CNT_THREE_PROCESS_3 = new ExecCnt();
  private static final ExecCnt EXEC_CNT_ONE_PROCESS_ONE_FAILURE = new ExecCnt();
  private static final ExecCnt EXEC_CNT_THREE_PROCESS_ONE_FAILURE_1 = new ExecCnt();
  private static final ExecCnt EXEC_CNT_THREE_PROCESS_ONE_FAILURE_2 = new ExecCnt();
  private static final ExecCnt EXEC_CNT_THREE_PROCESS_ONE_FAILURE_3 = new ExecCnt();

  public static class TestProcessFactoryOneProcess extends TestProcessFactory {
    public TestProcessFactoryOneProcess() {
      super(PIPELINE_NAME_ONE_PROCESS, EXEC_CNT_ONE_PROCESS, false);
    }
  }

  public static class TestProcessFactoryThreeProcesses1 extends TestProcessFactory {
    public TestProcessFactoryThreeProcesses1() {
      super(PIPELINE_NAME_THREE_PROCESS_1, EXEC_CNT_THREE_PROCESS_1, false);
    }
  }

  public static class TestProcessFactoryThreeProcesses2 extends TestProcessFactory {
    public TestProcessFactoryThreeProcesses2() {
      super(PIPELINE_NAME_THREE_PROCESS_2, EXEC_CNT_THREE_PROCESS_2, false);
    }
  }

  public static class TestProcessFactoryThreeProcesses3 extends TestProcessFactory {
    public TestProcessFactoryThreeProcesses3() {
      super(PIPELINE_NAME_THREE_PROCESS_3, EXEC_CNT_THREE_PROCESS_3, false);
    }
  }

  public static class TestProcessFactoryOneProcessOneFailure extends TestProcessFactory {
    public TestProcessFactoryOneProcessOneFailure() {
      super(PIPELINE_NAME_ONE_PROCESS_ONE_FAILURE, EXEC_CNT_ONE_PROCESS_ONE_FAILURE, true);
    }
  }

  public static class TestProcessFactoryThreeProcessesOneFailure1 extends TestProcessFactory {
    public TestProcessFactoryThreeProcessesOneFailure1() {
      super(PIPELINE_NAME_THREE_PROCESS_ONE_FAILURE_1, EXEC_CNT_THREE_PROCESS_ONE_FAILURE_1, false);
    }
  }

  public static class TestProcessFactoryThreeProcessesOneFailure2 extends TestProcessFactory {
    public TestProcessFactoryThreeProcessesOneFailure2() {
      super(PIPELINE_NAME_THREE_PROCESS_ONE_FAILURE_2, EXEC_CNT_THREE_PROCESS_ONE_FAILURE_2, false);
    }
  }

  public static class TestProcessFactoryThreeProcessesOneFailure3 extends TestProcessFactory {
    public TestProcessFactoryThreeProcessesOneFailure3() {
      super(PIPELINE_NAME_THREE_PROCESS_ONE_FAILURE_3, EXEC_CNT_THREE_PROCESS_ONE_FAILURE_3, true);
    }
  }

  private void saveSchedule(String pipelineName, int scheduleSeconds, String processFactoryName) {
    ScheduleEntity schedule = new ScheduleEntity();
    schedule.setSchedule("0/" + scheduleSeconds + " * * * * ?");
    schedule.setLauncherName(launcherConfiguration.getLauncherName());
    schedule.setPipelineName(pipelineName);
    schedule.setProcessFactoryName(processFactoryName);
    scheduleService.saveProcessSchedule(schedule);
    System.out.println(
        "saved schedule for pipeline: "
            + pipelineName
            + ", launcher: "
            + launcherConfiguration.getLauncherName());
  }

  private void deleteSchedule(String pipelineName) {
    ScheduleEntity schedule = new ScheduleEntity();
    schedule.setPipelineName(pipelineName);
    scheduleService.delete(schedule);
    System.out.println(
        "deleted schedule for pipeline: "
            + pipelineName
            + ", launcher: "
            + launcherConfiguration.getLauncherName());
  }

  public void assertResult(PipeliteScheduler pipeliteScheduler, List<TestResult> results) {

    int totalProcessCompletedCount = 0;
    int totalStageFailedCount = 0;
    int totalStageCompletedCount = 0;

    for (TestResult result : results) {

      totalProcessCompletedCount += result.execCnt.processExecutionCount.get();
      ;
      if (result.failure) {
        totalStageFailedCount += result.execCnt.stageExecutionCount.get();
      } else {
        totalStageCompletedCount += result.execCnt.stageExecutionCount.get();
      }

      // Minimum delay before first process is executed is ~0.
      // Maximum executed processes is estimated as: STOP_AFTER / scheduleSeconds[i] + 1
      // Maximum delay before first process is executed is ~scheduleSeconds[i].
      // processLaunchFrequency may prevent last scheduled process execution.
      // Minimum executed processes is estimated as STOP_AFTER / scheduleSeconds[i] - 2

      int expectedProcessExecCnt = (int) STOP_AFTER.toMillis() / 1000 / result.scheduleSeconds;
      int minExpectedProcessExecCnt = Math.max(0, expectedProcessExecCnt - 2);
      int maxExpectedProcessExecCnt = expectedProcessExecCnt + 1;
      int minExpectedStageExecCnt = minExpectedProcessExecCnt * 2;
      int maxExpectedStageExecCnt = maxExpectedProcessExecCnt * 2;

      assertThat(result.execCnt.processExecutionCount.get())
          .isBetween(minExpectedProcessExecCnt, maxExpectedProcessExecCnt);
      assertThat(result.execCnt.stageExecutionCount.get())
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
    EXEC_CNT_ONE_PROCESS.reset();

    try {
      saveSchedule(
          PIPELINE_NAME_ONE_PROCESS,
          SCHEDULE_SECONDS_ONE_PROCESS,
          TestProcessFactoryOneProcess.class.getName());

      pipeliteScheduler.setShutdownAfter(STOP_AFTER);

      ServerManager.run(pipeliteScheduler, pipeliteScheduler.serviceName());

      assertResult(
          pipeliteScheduler,
          Arrays.asList(new TestResult(SCHEDULE_SECONDS_ONE_PROCESS, EXEC_CNT_ONE_PROCESS, false)));
    } finally {
      deleteSchedule(PIPELINE_NAME_ONE_PROCESS);
    }
  }

  public void testThreeProcesses() {
    EXEC_CNT_THREE_PROCESS_1.reset();
    EXEC_CNT_THREE_PROCESS_2.reset();
    EXEC_CNT_THREE_PROCESS_3.reset();

    try {
      saveSchedule(
          PIPELINE_NAME_THREE_PROCESS_1,
          SCHEDULE_SECONDS_THREE_PROCESS_1,
          TestProcessFactoryThreeProcesses1.class.getName());
      saveSchedule(
          PIPELINE_NAME_THREE_PROCESS_2,
          SCHEDULE_SECONDS_THREE_PROCESS_2,
          TestProcessFactoryThreeProcesses2.class.getName());
      saveSchedule(
          PIPELINE_NAME_THREE_PROCESS_3,
          SCHEDULE_SECONDS_THREE_PROCESS_3,
          TestProcessFactoryThreeProcesses3.class.getName());

      pipeliteScheduler.setShutdownAfter(STOP_AFTER);

      ServerManager.run(pipeliteScheduler, pipeliteScheduler.serviceName());

      assertResult(
          pipeliteScheduler,
          Arrays.asList(
              new TestResult(SCHEDULE_SECONDS_THREE_PROCESS_1, EXEC_CNT_THREE_PROCESS_1, false),
              new TestResult(SCHEDULE_SECONDS_THREE_PROCESS_2, EXEC_CNT_THREE_PROCESS_2, false),
              new TestResult(SCHEDULE_SECONDS_THREE_PROCESS_3, EXEC_CNT_THREE_PROCESS_3, false)));

    } finally {
      deleteSchedule(PIPELINE_NAME_THREE_PROCESS_1);
      deleteSchedule(PIPELINE_NAME_THREE_PROCESS_2);
      deleteSchedule(PIPELINE_NAME_THREE_PROCESS_3);
    }
  }

  public void testOneProcessesOneFailure() {
    EXEC_CNT_ONE_PROCESS_ONE_FAILURE.reset();

    try {
      saveSchedule(
              PIPELINE_NAME_ONE_PROCESS_ONE_FAILURE,
              SCHEDULE_SECONDS_ONE_PROCESS_ONE_FAILURE,
              TestProcessFactoryOneProcessOneFailure.class.getName());

      pipeliteScheduler.setShutdownAfter(STOP_AFTER);

      ServerManager.run(pipeliteScheduler, pipeliteScheduler.serviceName());

      assertResult(
              pipeliteScheduler,
              Arrays.asList(
                      new TestResult(
                              SCHEDULE_SECONDS_ONE_PROCESS_ONE_FAILURE,
                              EXEC_CNT_ONE_PROCESS_ONE_FAILURE,
                              true)));

    } finally {
      deleteSchedule(PIPELINE_NAME_ONE_PROCESS_ONE_FAILURE);
    }
  }

  public void testThreeProcessesOneFailure() {
    EXEC_CNT_THREE_PROCESS_ONE_FAILURE_1.reset();
    EXEC_CNT_THREE_PROCESS_ONE_FAILURE_2.reset();
    EXEC_CNT_THREE_PROCESS_ONE_FAILURE_3.reset();

    try {
      saveSchedule(
          PIPELINE_NAME_THREE_PROCESS_ONE_FAILURE_1,
          SCHEDULE_SECONDS_THREE_PROCESS_ONE_FAILURE_1,
          TestProcessFactoryThreeProcessesOneFailure1.class.getName());
      saveSchedule(
          PIPELINE_NAME_THREE_PROCESS_ONE_FAILURE_2,
          SCHEDULE_SECONDS_THREE_PROCESS_ONE_FAILURE_2,
          TestProcessFactoryThreeProcessesOneFailure2.class.getName());
      saveSchedule(
          PIPELINE_NAME_THREE_PROCESS_ONE_FAILURE_3,
          SCHEDULE_SECONDS_THREE_PROCESS_ONE_FAILURE_3,
          TestProcessFactoryThreeProcessesOneFailure3.class.getName());

      pipeliteScheduler.setShutdownAfter(STOP_AFTER);

      ServerManager.run(pipeliteScheduler, pipeliteScheduler.serviceName());

      assertResult(
          pipeliteScheduler,
          Arrays.asList(
              new TestResult(
                  SCHEDULE_SECONDS_THREE_PROCESS_ONE_FAILURE_1,
                  EXEC_CNT_THREE_PROCESS_ONE_FAILURE_1,
                  false),
              new TestResult(
                  SCHEDULE_SECONDS_THREE_PROCESS_ONE_FAILURE_2,
                  EXEC_CNT_THREE_PROCESS_ONE_FAILURE_2,
                  false),
              new TestResult(
                  SCHEDULE_SECONDS_THREE_PROCESS_ONE_FAILURE_3,
                  EXEC_CNT_THREE_PROCESS_ONE_FAILURE_3,
                  true)));

    } finally {
      deleteSchedule(PIPELINE_NAME_THREE_PROCESS_ONE_FAILURE_1);
      deleteSchedule(PIPELINE_NAME_THREE_PROCESS_ONE_FAILURE_2);
      deleteSchedule(PIPELINE_NAME_THREE_PROCESS_ONE_FAILURE_3);
    }
  }
}
