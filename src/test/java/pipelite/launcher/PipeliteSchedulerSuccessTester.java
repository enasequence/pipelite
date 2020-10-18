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
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

@Component
@Scope("prototype")
public class PipeliteSchedulerSuccessTester {

  private final PipeliteScheduler pipeliteScheduler;
  private final ScheduleService scheduleService;
  private final LauncherConfiguration launcherConfiguration;

  public PipeliteSchedulerSuccessTester(
      @Autowired PipeliteScheduler pipeliteScheduler,
      @Autowired ScheduleService scheduleService,
      @Autowired LauncherConfiguration launcherConfiguration) {
    this.pipeliteScheduler = pipeliteScheduler;
    this.scheduleService = scheduleService;
    this.launcherConfiguration = launcherConfiguration;
  }

  private static final Duration STOP_AFTER = Duration.ofSeconds(10);

  private static class ExecutionCounter {
    public final AtomicInteger processExecutionCount = new AtomicInteger();
    public final AtomicInteger stageExecutionCount = new AtomicInteger();

    public void reset() {
      processExecutionCount.set(0);
      stageExecutionCount.set(0);
    }
  }

  public static class TestProcessFactory implements ProcessFactory {
    public final String pipelineName;
    public final ExecutionCounter executionCounter;

    public TestProcessFactory(String pipelineName, ExecutionCounter executionCounter) {
      this.pipelineName = pipelineName;
      this.executionCounter = executionCounter;
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
                executionCounter.processExecutionCount.incrementAndGet();
                executionCounter.stageExecutionCount.incrementAndGet();
                return StageExecutionResult.success();
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
                executionCounter.stageExecutionCount.incrementAndGet();
                return StageExecutionResult.success();
              })
          .build();
    }
  }

  private static final String PIPELINE_NAME_0 =
      UniqueStringGenerator.randomPipelineName(); //  "SCHEDULER_PIPELINE_0";
  private static final String PIPELINE_NAME_1 =
      UniqueStringGenerator.randomPipelineName(); //  "SCHEDULER_PIPELINE_1";
  private static final String PIPELINE_NAME_2 =
      UniqueStringGenerator.randomPipelineName(); // "SCHEDULER_PIPELINE_2";
  private static final String PIPELINE_NAME_3 =
      UniqueStringGenerator.randomPipelineName(); // "SCHEDULER_PIPELINE_3";

  private static final int SCHEDULE_SECONDS_0 = 2; // 60 must be divisible by SCHEDULE_SECONDS.
  private static final int SCHEDULE_SECONDS_1 = 2; // 60 must be divisible by SCHEDULE_SECONDS.
  private static final int SCHEDULE_SECONDS_2 = 4; // 60 must be divisible by SCHEDULE_SECONDS.
  private static final int SCHEDULE_SECONDS_3 = 6; // 60 must be divisible by SCHEDULE_SECONDS.

  private static final ExecutionCounter EXECUTION_COUNTER_0 = new ExecutionCounter();
  private static final ExecutionCounter EXECUTION_COUNTER_1 = new ExecutionCounter();
  private static final ExecutionCounter EXECUTION_COUNTER_2 = new ExecutionCounter();
  private static final ExecutionCounter EXECUTION_COUNTER_3 = new ExecutionCounter();

  public static class TestProcessFactory0 extends TestProcessFactory {
    public TestProcessFactory0() {
      super(PIPELINE_NAME_0, EXECUTION_COUNTER_0);
    }
  }

  public static class TestProcessFactory1 extends TestProcessFactory {
    public TestProcessFactory1() {
      super(PIPELINE_NAME_1, EXECUTION_COUNTER_1);
    }
  }

  public static class TestProcessFactory2 extends TestProcessFactory {
    public TestProcessFactory2() {
      super(PIPELINE_NAME_2, EXECUTION_COUNTER_2);
    }
  }

  public static class TestProcessFactory3 extends TestProcessFactory {
    public TestProcessFactory3() {
      super(PIPELINE_NAME_3, EXECUTION_COUNTER_3);
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

  public void assertResult(
      PipeliteScheduler pipeliteScheduler,
      int[] scheduleSeconds,
      ExecutionCounter[] executionCounters) {

    int totalMinExpectedProcessCompletedCount = 0;
    int totalMinExpectedStageCompletedCount = 0;
    int totalMaxExpectedProcessCompletedCount = 0;
    int totalMaxExpectedStageCompletedCount = 0;

    for (int i = 0; i < scheduleSeconds.length; ++i) {
      // Minimum delay before first process is executed is ~0.
      // Maximum executed processes is estimated as: STOP_AFTER / scheduleSeconds[i] + 1
      // Maximum delay before first process is executed is ~scheduleSeconds[i].
      // processLaunchFrequency may prevent last scheduled process execution.
      // Minimum executed processes is estimated as STOP_AFTER / scheduleSeconds[i] - 2

      int expectedProcessCompletedCount = (int) STOP_AFTER.toMillis() / 1000 / scheduleSeconds[i];

      int minExpectedProcessCompletedCount = expectedProcessCompletedCount - 2;
      int minExpectedStageCompletedCount = minExpectedProcessCompletedCount * 2;
      int maxExpectedProcessCompletedCount = expectedProcessCompletedCount + 1;
      int maxExpectedStageCompletedCount = maxExpectedProcessCompletedCount * 2;

      assertThat(executionCounters[i].processExecutionCount.get())
          .isBetween(minExpectedProcessCompletedCount, maxExpectedProcessCompletedCount);
      assertThat(executionCounters[i].stageExecutionCount.get())
          .isBetween(minExpectedStageCompletedCount, maxExpectedStageCompletedCount);

      totalMinExpectedProcessCompletedCount += minExpectedProcessCompletedCount;
      totalMinExpectedStageCompletedCount += minExpectedStageCompletedCount;
      totalMaxExpectedProcessCompletedCount += maxExpectedProcessCompletedCount;
      totalMaxExpectedStageCompletedCount += maxExpectedStageCompletedCount;
    }

    assertThat(pipeliteScheduler.getActiveProcessCount()).isEqualTo(0);
    assertThat(pipeliteScheduler.getProcessFailedToCreateCount()).isEqualTo(0);
    assertThat(pipeliteScheduler.getProcessFailedToExecuteCount()).isEqualTo(0);
    assertThat(pipeliteScheduler.getProcessCompletedCount())
        .isBetween(totalMinExpectedProcessCompletedCount, totalMaxExpectedProcessCompletedCount);
    assertThat(pipeliteScheduler.getStageFailedCount()).isEqualTo(0);
    assertThat(pipeliteScheduler.getStageCompletedCount())
        .isBetween(totalMinExpectedStageCompletedCount, totalMaxExpectedStageCompletedCount);
  }

  public void testSingleProcess() {
    EXECUTION_COUNTER_0.reset();

    try {
      saveSchedule(PIPELINE_NAME_0, SCHEDULE_SECONDS_0, TestProcessFactory0.class.getName());

      pipeliteScheduler.setShutdownAfter(STOP_AFTER);

      ServerManager.run(pipeliteScheduler, pipeliteScheduler.serviceName());

      assertResult(
          pipeliteScheduler,
          new int[] {SCHEDULE_SECONDS_0},
          new ExecutionCounter[] {EXECUTION_COUNTER_0});
    } finally {
      deleteSchedule(PIPELINE_NAME_0);
    }
  }

  public void testThreeProcesses() {
    EXECUTION_COUNTER_1.reset();
    EXECUTION_COUNTER_2.reset();
    EXECUTION_COUNTER_3.reset();

    try {
      saveSchedule(PIPELINE_NAME_1, SCHEDULE_SECONDS_1, TestProcessFactory1.class.getName());
      saveSchedule(PIPELINE_NAME_2, SCHEDULE_SECONDS_2, TestProcessFactory2.class.getName());
      saveSchedule(PIPELINE_NAME_3, SCHEDULE_SECONDS_3, TestProcessFactory3.class.getName());

      pipeliteScheduler.setShutdownAfter(STOP_AFTER);

      ServerManager.run(pipeliteScheduler, pipeliteScheduler.serviceName());

      assertResult(
          pipeliteScheduler,
          new int[] {SCHEDULE_SECONDS_1, SCHEDULE_SECONDS_2, SCHEDULE_SECONDS_3},
          new ExecutionCounter[] {EXECUTION_COUNTER_1, EXECUTION_COUNTER_2, EXECUTION_COUNTER_3});
    } finally {
      deleteSchedule(PIPELINE_NAME_1);
      deleteSchedule(PIPELINE_NAME_2);
      deleteSchedule(PIPELINE_NAME_3);
    }
  }
}
