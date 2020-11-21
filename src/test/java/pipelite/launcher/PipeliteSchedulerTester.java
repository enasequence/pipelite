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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import lombok.Value;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;
import pipelite.UniqueStringGenerator;
import pipelite.configuration.LauncherConfiguration;
import pipelite.entity.ProcessEntity;
import pipelite.entity.ScheduleEntity;
import pipelite.entity.StageEntity;
import pipelite.process.Process;
import pipelite.process.ProcessFactory;
import pipelite.process.ProcessState;
import pipelite.process.builder.ProcessBuilder;
import pipelite.service.ProcessService;
import pipelite.service.ScheduleService;
import pipelite.service.StageService;
import pipelite.stage.StageExecutionResult;
import pipelite.stage.StageExecutionResultType;
import pipelite.stage.StageParameters;

@Component
@Scope("prototype")
public class PipeliteSchedulerTester {

  private final PipeliteScheduler pipeliteScheduler;
  private final ScheduleService scheduleService;
  private final LauncherConfiguration launcherConfiguration;
  private final ProcessService processService;
  private final StageService stageService;

  @Autowired private TestProcessFactory firstProcessSuccess;
  @Autowired private TestProcessFactory secondProcessSuccess;
  @Autowired private TestProcessFactory thirdProcessSuccess;
  @Autowired private TestProcessFactory firstProcessFailure;
  @Autowired private TestProcessFactory secondProcessFailure;
  @Autowired private TestProcessFactory thirdProcessFailure;
  @Autowired private TestProcessFactory firstProcessException;
  @Autowired private TestProcessFactory secondProcessException;
  @Autowired private TestProcessFactory thirdProcessException;

  public PipeliteSchedulerTester(
      @Autowired PipeliteScheduler pipeliteScheduler,
      @Autowired ScheduleService scheduleService,
      @Autowired LauncherConfiguration launcherConfiguration,
      @Autowired ProcessService processService,
      @Autowired StageService stageService) {
    this.pipeliteScheduler = pipeliteScheduler;
    this.scheduleService = scheduleService;
    this.launcherConfiguration = launcherConfiguration;
    this.processService = processService;
    this.stageService = stageService;
  }

  @TestConfiguration
  static class TestConfig {
    @Bean
    public ProcessFactory firstProcessSuccess() {
      return new TestProcessFactory("firstProcessSuccess", 2, 2, 4, StageTestResult.SUCCESS);
    }

    @Bean
    public ProcessFactory secondProcessSuccess() {
      return new TestProcessFactory("secondProcessSuccess", 5, 4, 3, StageTestResult.SUCCESS);
    }

    @Bean
    public ProcessFactory thirdProcessSuccess() {
      return new TestProcessFactory("thirdProcessSuccess", 10, 6, 2, StageTestResult.SUCCESS);
    }

    @Bean
    public ProcessFactory firstProcessFailure() {
      return new TestProcessFactory("firstProcessFailure", 2, 2, 4, StageTestResult.ERROR);
    }

    @Bean
    public ProcessFactory secondProcessFailure() {
      return new TestProcessFactory("secondProcessFailure", 5, 4, 3, StageTestResult.ERROR);
    }

    @Bean
    public ProcessFactory thirdProcessFailure() {
      return new TestProcessFactory("thirdProcessFailure", 10, 6, 2, StageTestResult.ERROR);
    }

    @Bean
    public ProcessFactory firstProcessException() {
      return new TestProcessFactory("firstProcessException", 2, 2, 4, StageTestResult.EXCEPTION);
    }

    @Bean
    public ProcessFactory secondProcessException() {
      return new TestProcessFactory("secondProcessException", 5, 4, 3, StageTestResult.EXCEPTION);
    }

    @Bean
    public ProcessFactory thirdProcessException() {
      return new TestProcessFactory("thirdProcessException", 10, 6, 2, StageTestResult.EXCEPTION);
    }
  }

  private enum StageTestResult {
    SUCCESS,
    ERROR,
    EXCEPTION
  }

  @Value
  public static class TestProcessFactory implements ProcessFactory {
    private final String pipelineName;
    private final int stageCnt;
    public final int schedulerSeconds; // 60 must be divisible by schedulerSeconds.
    public final int schedulerMaxExecutions;
    public final StageTestResult stageTestResult;
    public final List<String> processIds = new ArrayList<>();
    public final AtomicLong stageExecCnt = new AtomicLong();

    public TestProcessFactory(
        String pipelineNamePrefix,
        int stageCnt,
        int schedulerSeconds,
        int scheduleMaxExecutions,
        StageTestResult stageTestResult) {
      this.pipelineName = pipelineNamePrefix + "_" + UniqueStringGenerator.randomPipelineName();
      this.stageCnt = stageCnt;
      this.schedulerSeconds = schedulerSeconds;
      this.schedulerMaxExecutions = scheduleMaxExecutions;
      this.stageTestResult = stageTestResult;
    }

    public void reset() {
      processIds.clear();
      stageExecCnt.set(0L);
    }

    @Override
    public String getPipelineName() {
      return pipelineName;
    }

    @Override
    public Process create(String processId) {
      processIds.add(processId);
      StageParameters stageParams =
          StageParameters.builder().immediateRetries(0).maximumRetries(0).build();

      ProcessBuilder processBuilder = new ProcessBuilder(processId);
      for (int i = 0; i < stageCnt; ++i) {
        processBuilder
            .execute("STAGE" + i, stageParams)
            .with(
                (pipelineName, processId1, stage) -> {
                  stageExecCnt.incrementAndGet();
                  if (stageTestResult == StageTestResult.ERROR) {
                    return StageExecutionResult.error();
                  }
                  if (stageTestResult == StageTestResult.SUCCESS) {
                    return StageExecutionResult.success();
                  }
                  if (stageTestResult == StageTestResult.EXCEPTION) {
                    throw new RuntimeException("Expected exception");
                  }
                  throw new RuntimeException("Unexpected exception");
                });
      }
      return processBuilder.build();
    }
  }

  private void saveSchedule(TestProcessFactory testProcessFactory) {
    ScheduleEntity schedule = new ScheduleEntity();
    schedule.setSchedule("0/" + testProcessFactory.schedulerSeconds + " * * * * ?");
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

      assertThat(f.processIds.size()).isEqualTo(f.schedulerMaxExecutions);

      for (String processId : f.processIds) {

        // Assert ProcessEntity

        ProcessEntity processEntity =
            processService.getSavedProcess(f.getPipelineName(), processId).get();
        assertThat(processEntity.getPipelineName()).isEqualTo(pipelineName);
        assertThat(processEntity.getProcessId()).isEqualTo(processId);
        assertThat(processEntity.getExecutionCount()).isEqualTo(1);
        if (f.stageTestResult != StageTestResult.SUCCESS) {
          assertThat(processEntity.getState())
              .isEqualTo(ProcessState.FAILED); // no re-executions allowed
        } else {
          assertThat(processEntity.getState()).isEqualTo(ProcessState.COMPLETED);
        }

        for (int i = 0; i < f.stageCnt; ++i) {

          // Assert StageEntity

          StageEntity stageEntity =
              stageService.getSavedStage(f.getPipelineName(), processId, "STAGE" + i).get();
          assertThat(stageEntity.getPipelineName()).isEqualTo(pipelineName);
          assertThat(stageEntity.getProcessId()).isEqualTo(processId);
          assertThat(stageEntity.getExecutionCount()).isEqualTo(1);
          assertThat(stageEntity.getStartTime()).isNotNull();
          assertThat(stageEntity.getEndTime()).isNotNull();
          assertThat(stageEntity.getStartTime()).isBeforeOrEqualTo(stageEntity.getEndTime());
          assertThat(stageEntity.getExecutorName())
              .startsWith(PipeliteSchedulerTester.class.getName());
          assertThat(stageEntity.getExecutorData()).isEqualTo("{ }");
          assertThat(stageEntity.getExecutorParams())
              .isEqualTo("{\n  \"maximumRetries\" : 0,\n  \"immediateRetries\" : 0\n}");

          if (f.stageTestResult == StageTestResult.ERROR) {
            assertThat(stageEntity.getResultType()).isEqualTo(StageExecutionResultType.ERROR);
            assertThat(stageEntity.getResultParams()).isNull();
          } else if (f.stageTestResult == StageTestResult.EXCEPTION) {
            assertThat(stageEntity.getResultType()).isEqualTo(StageExecutionResultType.ERROR);
            assertThat(stageEntity.getResultParams())
                .contains("exception\" : \"java.lang.RuntimeException: Expected exception");
          } else {
            assertThat(stageEntity.getResultType()).isEqualTo(StageExecutionResultType.SUCCESS);
            assertThat(stageEntity.getResultParams()).isNull();
          }
        }
      }

      // Assert PipeliteSchedulerStats

      assertThat(f.stageExecCnt.get() / f.stageCnt).isEqualTo(f.schedulerMaxExecutions);

      PipeliteSchedulerStats stats = pipeliteScheduler.getStats(pipelineName);

      assertThat(stats.getProcessCreationFailedCount()).isEqualTo(0);
      assertThat(stats.getProcessExceptionCount()).isEqualTo(0);

      if (f.stageTestResult != StageTestResult.SUCCESS) {
        assertThat(stats.getProcessExecutionCount(ProcessState.FAILED))
            .isEqualTo(f.stageExecCnt.get() / f.stageCnt);
        assertThat(stats.getStageFailedCount()).isEqualTo(f.stageExecCnt.get());
        assertThat(stats.getStageSuccessCount()).isEqualTo(0L);
        assertThat(stats.getStageSuccessCount()).isEqualTo(0L);

      } else {
        assertThat(stats.getProcessExecutionCount(ProcessState.COMPLETED))
            .isEqualTo(f.stageExecCnt.get() / f.stageCnt);
        assertThat(stats.getStageFailedCount()).isEqualTo(0L);
        assertThat(stats.getStageFailedCount()).isEqualTo(0L);
        assertThat(stats.getStageSuccessCount()).isEqualTo(f.stageExecCnt.get());
      }
    }

    assertThat(pipeliteScheduler.getActivePipelinesCount()).isEqualTo(0);
  }

  private void test(List<TestProcessFactory> testProcessFactories) {
    try {
      for (TestProcessFactory f : testProcessFactories) {
        f.reset();
        saveSchedule(f);
        pipeliteScheduler.setMaximumExecutions(f.getPipelineName(), f.schedulerMaxExecutions);
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

  public void testOneFailureSchedule() {
    test(Arrays.asList(firstProcessFailure));
  }

  public void testThreeFailureSchedules() {
    test(Arrays.asList(firstProcessFailure, secondProcessFailure, thirdProcessFailure));
  }

  public void testOneExceptionSchedule() {
    test(Arrays.asList(firstProcessException));
  }

  public void testThreeExceptionSchedules() {
    test(Arrays.asList(firstProcessException, secondProcessException, thirdProcessException));
  }

  public void testOneSuccessOneFailureOneExceptionSchedule() {
    test(Arrays.asList(firstProcessSuccess, firstProcessFailure));
  }

  public void testTwoSuccessTwoFailureTwoExceptionSchedule() {
    test(
        Arrays.asList(
            firstProcessSuccess,
            secondProcessSuccess,
            firstProcessFailure,
            secondProcessFailure,
            firstProcessException,
            secondProcessException));
  }
}
