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
import java.util.concurrent.atomic.AtomicLong;

import lombok.Value;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;
import org.springframework.test.annotation.DirtiesContext;
import pipelite.UniqueStringGenerator;
import pipelite.configuration.LauncherConfiguration;
import pipelite.entity.ScheduleEntity;
import pipelite.process.Process;
import pipelite.process.ProcessFactory;
import pipelite.process.ProcessState;
import pipelite.process.builder.ProcessBuilder;
import pipelite.service.ScheduleService;
import pipelite.stage.StageExecutionResult;
import pipelite.stage.StageParameters;

@Component
@Scope("prototype")
public class PipeliteSchedulerTester {

  private final PipeliteScheduler pipeliteScheduler;
  private final ScheduleService scheduleService;
  private final LauncherConfiguration launcherConfiguration;

  @Autowired private TestProcessFactory firstProcessSuccess;
  @Autowired private TestProcessFactory secondProcessSuccess;
  @Autowired private TestProcessFactory thirdProcessSuccess;
  @Autowired private TestProcessFactory firstProcessFailure;
  @Autowired private TestProcessFactory secondProcessFailure;
  @Autowired private TestProcessFactory thirdProcessFailure;

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
    public ProcessFactory firstProcessSuccess() {
      return new TestProcessFactory("firstProcessSuccess", 2, 4, false);
    }

    @Bean
    public ProcessFactory secondProcessSuccess() {
      return new TestProcessFactory("secondProcessSuccess", 4, 3, false);
    }

    @Bean
    public ProcessFactory thirdProcessSuccess() {
      return new TestProcessFactory("thirdProcessSuccess", 6, 2, false);
    }

    @Bean
    public ProcessFactory firstProcessFailure() {
      return new TestProcessFactory("firstProcessFailure", 2, 4, true);
    }

    @Bean
    public ProcessFactory secondProcessFailure() {
      return new TestProcessFactory("secondProcessFailure", 4, 3, true);
    }

    @Bean
    public ProcessFactory thirdProcessFailure() {
      return new TestProcessFactory("thirdProcessFailure", 6, 2, true);
    }
  }

  private static final Duration STOP_AFTER = Duration.ofSeconds(10);

  @Value
  public static class TestProcessFactory implements ProcessFactory {
    private final String pipelineName;
    public final int seconds; // 60 must be divisible by seconds.
    public final int maxExecutions;
    public final boolean failure;
    public final AtomicLong processExecCnt = new AtomicLong();
    public final AtomicLong stageExecCnt = new AtomicLong();

    public TestProcessFactory(
        String pipelineNamePrefix, int seconds, int maxExecutions, boolean failure) {
      this.pipelineName = pipelineNamePrefix + "_" + UniqueStringGenerator.randomPipelineName();
      this.seconds = seconds;
      this.maxExecutions = maxExecutions;
      this.failure = failure;
    }

    public void reset() {
      processExecCnt.set(0L);
      stageExecCnt.set(0L);
    }

    @Override
    public String getPipelineName() {
      return pipelineName;
    }

    private static final StageParameters STAGE_PARAMS =
        StageParameters.builder().immediateRetries(0).maximumRetries(0).build();

    @Override
    public Process create(String processId) {
      return new ProcessBuilder(processId)
          .execute("STAGE1", STAGE_PARAMS)
          .with(
              (pipelineName, processId1, stage) -> {
                processExecCnt.incrementAndGet();
                stageExecCnt.incrementAndGet();
                if (failure) {
                  return StageExecutionResult.error();
                } else {
                  return StageExecutionResult.success();
                }
              })
          .execute("STAGE2", STAGE_PARAMS)
          .with(
              (pipelineName, processId1, stage) -> {
                stageExecCnt.incrementAndGet();
                if (failure) {
                  return StageExecutionResult.error();
                } else {
                  return StageExecutionResult.success();
                }
              })
          .build();
    }
  }

  private void saveSchedule(TestProcessFactory testProcessFactory) {
    ScheduleEntity schedule = new ScheduleEntity();
    schedule.setSchedule("0/" + testProcessFactory.seconds + " * * * * ?");
    schedule.setLauncherName(launcherConfiguration.getLauncherName());
    schedule.setPipelineName(testProcessFactory.pipelineName);
    scheduleService.saveProcessSchedule(schedule);
    System.out.println(
        "saved schedule for pipeline: "
            + testProcessFactory.pipelineName
            + ", launcher: "
            + launcherConfiguration.getLauncherName());
  }

  private void deleteSchedule(TestProcessFactory testProcessFactory) {
    ScheduleEntity schedule = new ScheduleEntity();
    schedule.setPipelineName(testProcessFactory.pipelineName);
    scheduleService.delete(schedule);
    System.out.println(
        "deleted schedule for pipeline: "
            + testProcessFactory.pipelineName
            + ", launcher: "
            + launcherConfiguration.getLauncherName());
  }

  public void assertResult(
      PipeliteScheduler pipeliteScheduler, List<TestProcessFactory> testProcessFactories) {

    for (TestProcessFactory f : testProcessFactories) {
      String pipelineName = f.getPipelineName();

      assertThat(f.processExecCnt.get()).isEqualTo(f.maxExecutions);

      PipeliteSchedulerStats stats = pipeliteScheduler.getStats(pipelineName);

      assertThat(stats.getProcessCreationFailedCount()).isEqualTo(0);
      assertThat(stats.getProcessExceptionCount()).isEqualTo(0);

      if (f.failure) {
        assertThat(stats.getProcessExecutionCount(ProcessState.FAILED))
            .isEqualTo(f.processExecCnt.get());
        assertThat(stats.getStageFailedCount()).isEqualTo(f.processExecCnt.get() * 2);
        assertThat(stats.getStageFailedCount()).isEqualTo(f.stageExecCnt.get());
        assertThat(stats.getStageSuccessCount()).isEqualTo(0L);
        assertThat(stats.getStageSuccessCount()).isEqualTo(0L);

      } else {
        assertThat(stats.getProcessExecutionCount(ProcessState.COMPLETED))
            .isEqualTo(f.processExecCnt.get());
        assertThat(stats.getStageFailedCount()).isEqualTo(0L);
        assertThat(stats.getStageFailedCount()).isEqualTo(0L);
        assertThat(stats.getStageSuccessCount()).isEqualTo(f.processExecCnt.get() * 2);
        assertThat(stats.getStageSuccessCount()).isEqualTo(f.stageExecCnt.get());
      }
    }

    assertThat(pipeliteScheduler.getActiveProcessCount()).isEqualTo(0);
  }

  private void test(List<TestProcessFactory> testProcessFactories) {
    try {
      for (TestProcessFactory f : testProcessFactories) {
        f.reset();
        saveSchedule(f);
        pipeliteScheduler.setMaximumExecutions(f.getPipelineName(), f.maxExecutions);
      }
      ServerManager.run(pipeliteScheduler, pipeliteScheduler.serviceName());
      assertResult(pipeliteScheduler, testProcessFactories);
    } finally {
      for (TestProcessFactory f : testProcessFactories) {
        deleteSchedule(f);
      }
    }
  }

  public void testOneSuccessSchedule() {
    test(Arrays.asList(firstProcessSuccess));
  }

  public void testThreeSuccessSchedules() {
    test(Arrays.asList(firstProcessSuccess, secondProcessSuccess, thirdProcessSuccess));
  }

  public void testOneSuccessOneFailureSchedule() {
    test(Arrays.asList(firstProcessSuccess, firstProcessFailure));
  }

  public void testOneFailureSchedule() {
    test(Arrays.asList(firstProcessFailure));
  }

  public void testThreeFailureSchedules() {
    test(Arrays.asList(firstProcessFailure, secondProcessFailure, thirdProcessFailure));
  }
}
