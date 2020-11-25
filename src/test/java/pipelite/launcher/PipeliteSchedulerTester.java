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
import java.util.Collections;
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
import pipelite.executor.StageExecutorParameters;

@Component
@Scope("prototype")
public class PipeliteSchedulerTester {

  @Autowired private PipeliteScheduler pipeliteScheduler;
  @Autowired private ScheduleService scheduleService;
  @Autowired private LauncherConfiguration launcherConfiguration;
  @Autowired private ProcessService processService;
  @Autowired private StageService stageService;

  @Autowired private TestProcessFactory firstProcessSuccess;
  @Autowired private TestProcessFactory secondProcessSuccess;
  @Autowired private TestProcessFactory thirdProcessSuccess;
  @Autowired private TestProcessFactory firstProcessFailure;
  @Autowired private TestProcessFactory secondProcessFailure;
  @Autowired private TestProcessFactory thirdProcessFailure;
  @Autowired private TestProcessFactory firstProcessException;
  @Autowired private TestProcessFactory secondProcessException;
  @Autowired private TestProcessFactory thirdProcessException;

  @TestConfiguration
  static class TestConfig {
    @Bean
    public ProcessFactory firstProcessSuccess() {
      return new TestProcessFactory("firstProcessSuccess", 4, 2, 2, StageTestResult.SUCCESS);
    }

    @Bean
    public ProcessFactory secondProcessSuccess() {
      return new TestProcessFactory("secondProcessSuccess", 3, 3, 4, StageTestResult.SUCCESS);
    }

    @Bean
    public ProcessFactory thirdProcessSuccess() {
      return new TestProcessFactory("thirdProcessSuccess", 2, 4, 6, StageTestResult.SUCCESS);
    }

    @Bean
    public ProcessFactory firstProcessFailure() {
      return new TestProcessFactory("firstProcessFailure", 4, 2, 2, StageTestResult.ERROR);
    }

    @Bean
    public ProcessFactory secondProcessFailure() {
      return new TestProcessFactory("secondProcessFailure", 3, 3, 4, StageTestResult.ERROR);
    }

    @Bean
    public ProcessFactory thirdProcessFailure() {
      return new TestProcessFactory("thirdProcessFailure", 2, 4, 6, StageTestResult.ERROR);
    }

    @Bean
    public ProcessFactory firstProcessException() {
      return new TestProcessFactory("firstProcessException", 4, 2, 2, StageTestResult.EXCEPTION);
    }

    @Bean
    public ProcessFactory secondProcessException() {
      return new TestProcessFactory("secondProcessException", 3, 3, 4, StageTestResult.EXCEPTION);
    }

    @Bean
    public ProcessFactory thirdProcessException() {
      return new TestProcessFactory("thirdProcessException", 2, 4, 6, StageTestResult.EXCEPTION);
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
    public final int processCnt;
    public final int stageCnt;
    public final int schedulerSeconds; // 60 must be divisible by schedulerSeconds.
    public final StageTestResult stageTestResult;
    public final String cronExpression;
    public final List<String> processIds = Collections.synchronizedList(new ArrayList<>());
    public final AtomicLong stageExecCnt = new AtomicLong();

    public TestProcessFactory(
        String pipelineNamePrefix,
        int processCnt,
        int stageCnt,
        int schedulerSeconds,
        StageTestResult stageTestResult) {
      this.pipelineName = pipelineNamePrefix + "_" + UniqueStringGenerator.randomPipelineName();
      this.stageCnt = stageCnt;
      this.schedulerSeconds = schedulerSeconds;
      this.processCnt = processCnt;
      this.stageTestResult = stageTestResult;
      this.cronExpression = "0/" + schedulerSeconds + " * * * * ?";
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
      StageExecutorParameters executorParams =
          StageExecutorParameters.builder().immediateRetries(0).maximumRetries(0).build();

      ProcessBuilder processBuilder = new ProcessBuilder(processId);
      for (int i = 0; i < stageCnt; ++i) {
        processBuilder
            .execute("STAGE" + i, executorParams)
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
    schedule.setSchedule(testProcessFactory.cronExpression);
    schedule.setSchedulerName(launcherConfiguration.getSchedulerName());
    schedule.setPipelineName(testProcessFactory.pipelineName);
    scheduleService.saveProcessSchedule(schedule);
    System.out.println(
        "saved schedule for pipeline: "
            + testProcessFactory.pipelineName
            + ", scheduler: "
            + launcherConfiguration.getSchedulerName());
  }

  private void deleteSchedule(TestProcessFactory testProcessFactory) {
    ScheduleEntity schedule = new ScheduleEntity();
    schedule.setPipelineName(testProcessFactory.pipelineName);
    scheduleService.delete(schedule);
    System.out.println(
        "deleted schedule for pipeline: "
            + testProcessFactory.pipelineName
            + ", scheduler: "
            + launcherConfiguration.getSchedulerName());
  }

  private void assertSchedulerStats(PipeliteScheduler pipeliteScheduler, TestProcessFactory f) {
    String pipelineName = f.getPipelineName();

    PipeliteSchedulerStats stats = pipeliteScheduler.getStats(pipelineName);

    assertThat(stats.getProcessCreationFailedCount()).isEqualTo(0);
    assertThat(stats.getProcessExceptionCount()).isEqualTo(0);

    if (f.stageTestResult != StageTestResult.SUCCESS) {
      assertThat(stats.getProcessExecutionCount(ProcessState.FAILED))
          .isEqualTo(f.stageExecCnt.get() / f.stageCnt);
      assertThat(stats.getStageFailedCount()).isEqualTo(f.stageExecCnt.get());
      assertThat(stats.getStageSuccessCount()).isEqualTo(0L);

    } else {
      assertThat(stats.getProcessExecutionCount(ProcessState.COMPLETED))
          .isEqualTo(f.stageExecCnt.get() / f.stageCnt);
      assertThat(stats.getStageFailedCount()).isEqualTo(0L);
      assertThat(stats.getStageSuccessCount()).isEqualTo(f.stageExecCnt.get());
    }
  }

  private void assertScheduleEntity(List<ScheduleEntity> scheduleEntities, TestProcessFactory f) {
    String pipelineName = f.getPipelineName();

    assertThat(
            scheduleEntities.stream()
                .filter(e -> e.getPipelineName().equals(f.getPipelineName()))
                .count())
        .isEqualTo(1);
    ScheduleEntity scheduleEntity =
        scheduleEntities.stream()
            .filter(e -> e.getPipelineName().equals(f.getPipelineName()))
            .findFirst()
            .get();
    assertThat(scheduleEntity.getSchedulerName())
        .isEqualTo(launcherConfiguration.getSchedulerName());
    assertThat(scheduleEntity.getPipelineName()).isEqualTo(pipelineName);
    assertThat(scheduleEntity.getProcessId())
        .isEqualTo(f.getProcessIds().get(f.getProcessIds().size() - 1));
    assertThat(scheduleEntity.getExecutionCount()).isEqualTo(f.processCnt);
    assertThat(scheduleEntity.getSchedule()).isEqualTo(f.cronExpression);
    assertThat(scheduleEntity.getStartTime()).isNotNull();
    assertThat(scheduleEntity.getEndTime()).isNotNull();
    assertThat(scheduleEntity.getStartTime()).isBeforeOrEqualTo(scheduleEntity.getEndTime());
    assertThat(scheduleEntity.getDescription())
        .isEqualTo("every " + f.schedulerSeconds + " seconds");
  }

  private void assertProcessEntity(TestProcessFactory f, String processId) {
    String pipelineName = f.getPipelineName();

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
  }

  private void assertStageEntities(TestProcessFactory f, String processId) {
    String pipelineName = f.getPipelineName();

    for (int i = 0; i < f.stageCnt; ++i) {
      StageEntity stageEntity =
          stageService.getSavedStage(f.getPipelineName(), processId, "STAGE" + i).get();
      assertThat(stageEntity.getPipelineName()).isEqualTo(pipelineName);
      assertThat(stageEntity.getProcessId()).isEqualTo(processId);
      assertThat(stageEntity.getExecutionCount()).isEqualTo(1);
      assertThat(stageEntity.getStartTime()).isNotNull();
      assertThat(stageEntity.getEndTime()).isNotNull();
      assertThat(stageEntity.getStartTime()).isBeforeOrEqualTo(stageEntity.getEndTime());
      assertThat(stageEntity.getExecutorName()).startsWith(PipeliteSchedulerTester.class.getName());
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

  private void test(List<TestProcessFactory> testProcessFactories) {
    try {
      for (TestProcessFactory f : testProcessFactories) {
        f.reset();
        saveSchedule(f);
        pipeliteScheduler.setMaximumExecutions(f.getPipelineName(), f.processCnt);
      }
      ServerManager.run(pipeliteScheduler, pipeliteScheduler.serviceName());

      assertThat(pipeliteScheduler.getActivePipelinesCount()).isEqualTo(0);
      List<ScheduleEntity> scheduleEntities =
          scheduleService.getAllProcessSchedules(launcherConfiguration.getSchedulerName());
      for (TestProcessFactory f : testProcessFactories) {
        assertThat(f.stageExecCnt.get() / f.stageCnt).isEqualTo(f.processCnt);
        assertThat(f.processIds.size()).isEqualTo(f.processCnt);
        assertSchedulerStats(pipeliteScheduler, f);
        assertScheduleEntity(scheduleEntities, f);
        for (String processId : f.processIds) {
          assertProcessEntity(f, processId);
          assertStageEntities(f, processId);
        }
      }

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
