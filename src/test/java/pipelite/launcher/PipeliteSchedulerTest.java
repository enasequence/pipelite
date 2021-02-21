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

import lombok.Value;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import pipelite.PipeliteTestConfiguration;
import pipelite.Schedule;
import pipelite.UniqueStringGenerator;
import pipelite.configuration.AdvancedConfiguration;
import pipelite.configuration.ExecutorConfiguration;
import pipelite.configuration.ServiceConfiguration;
import pipelite.entity.ProcessEntity;
import pipelite.entity.ScheduleEntity;
import pipelite.entity.StageEntity;
import pipelite.entity.StageLogEntity;
import pipelite.metrics.PipelineMetrics;
import pipelite.metrics.PipeliteMetrics;
import pipelite.metrics.TimeSeriesMetrics;
import pipelite.process.ProcessState;
import pipelite.process.builder.ProcessBuilder;
import pipelite.service.*;
import pipelite.stage.StageState;
import pipelite.stage.executor.StageExecutorResult;
import pipelite.stage.parameters.ExecutorParameters;
import pipelite.time.Time;

import java.time.Duration;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.spy;
import static org.springframework.test.annotation.DirtiesContext.ClassMode.BEFORE_EACH_TEST_METHOD;

@SpringBootTest(
    classes = PipeliteTestConfiguration.class,
    properties = {"pipelite.advanced.processRunnerFrequency=250ms"})
@ContextConfiguration(initializers = PipeliteTestConfiguration.TestContextInitializer.class)
@DirtiesContext(classMode = BEFORE_EACH_TEST_METHOD)
public class PipeliteSchedulerTest {

  @Autowired private ServiceConfiguration serviceConfiguration;
  @Autowired private AdvancedConfiguration advancedConfiguration;
  @Autowired private ExecutorConfiguration executorConfiguration;
  @Autowired private InternalErrorService internalErrorService;
  @Autowired private HealthCheckService healthCheckService;
  @Autowired private RegisteredPipelineService registeredPipelineService;
  @Autowired private ScheduleService scheduleService;
  @Autowired private ProcessService processService;
  @Autowired private StageService stageService;
  @Autowired private PipeliteLockerService pipeliteLockerService;
  @Autowired private MailService mailService;

  @Autowired private TestSchedule firstProcessSuccess;
  @Autowired private TestSchedule secondProcessSuccess;
  @Autowired private TestSchedule firstProcessFailure;
  @Autowired private TestSchedule secondProcessFailure;
  @Autowired private TestSchedule firstProcessException;
  @Autowired private TestSchedule secondProcessException;
  @Autowired private TestSchedule resume1;
  @Autowired private TestSchedule resume2;
  @Autowired private PipeliteMetrics metrics;

  @TestConfiguration
  static class TestConfig {
    @Bean
    public TestSchedule firstProcessSuccess() {
      return new TestSchedule(2, 1, 2, StageTestResult.SUCCESS);
    }

    @Bean
    public TestSchedule secondProcessSuccess() {
      return new TestSchedule(1, 2, 4, StageTestResult.SUCCESS);
    }

    @Bean
    public TestSchedule firstProcessFailure() {
      return new TestSchedule(2, 1, 2, StageTestResult.ERROR);
    }

    @Bean
    public TestSchedule secondProcessFailure() {
      return new TestSchedule(1, 2, 4, StageTestResult.ERROR);
    }

    @Bean
    public TestSchedule firstProcessException() {
      return new TestSchedule(2, 1, 2, StageTestResult.EXCEPTION);
    }

    @Bean
    public TestSchedule secondProcessException() {
      return new TestSchedule(1, 2, 4, StageTestResult.EXCEPTION);
    }

    @Bean
    public TestSchedule resume1() {
      return new TestSchedule(1, 2, 2, StageTestResult.SUCCESS);
    }

    @Bean
    public TestSchedule resume2() {
      return new TestSchedule(1, 2, 1, StageTestResult.SUCCESS);
    }
  }

  private enum StageTestResult {
    SUCCESS,
    ERROR,
    EXCEPTION
  }

  private PipeliteServices pipeliteServices() {
    return new PipeliteServices(
        scheduleService,
        processService,
        stageService,
        mailService,
        pipeliteLockerService,
        registeredPipelineService,
        internalErrorService,
        healthCheckService);
  }

  private PipeliteScheduler createPipeliteScheduler() {
    PipeliteConfiguration pipeliteConfiguration =
        new PipeliteConfiguration(
            serviceConfiguration, advancedConfiguration, executorConfiguration, metrics);
    return DefaultPipeliteScheduler.create(
        pipeliteConfiguration,
        pipeliteServices(),
        registeredPipelineService.getRegisteredPipelines(Schedule.class));
  }

  @Value
  public static class TestSchedule implements Schedule {
    private final String pipelineName;
    public final int processCnt;
    public final int stageCnt;
    public final int schedulerSeconds; // 60 must be divisible by schedulerSeconds.
    public final StageTestResult stageTestResult;
    public final String cron;
    public final List<String> processIds = Collections.synchronizedList(new ArrayList<>());
    public final AtomicLong stageExecCnt = new AtomicLong();

    public TestSchedule(
        int processCnt, int stageCnt, int schedulerSeconds, StageTestResult stageTestResult) {
      this.pipelineName = UniqueStringGenerator.randomPipelineName(PipeliteSchedulerTest.class);
      this.stageCnt = stageCnt;
      this.schedulerSeconds = schedulerSeconds;
      this.processCnt = processCnt;
      this.stageTestResult = stageTestResult;
      this.cron = "0/" + schedulerSeconds + " * * * * ?";
    }

    public void reset() {
      processIds.clear();
      stageExecCnt.set(0L);
    }

    @Override
    public String pipelineName() {
      return pipelineName;
    }

    @Override
    public Options configurePipeline() {
      return new Options().cron(cron);
    }

    @Override
    public void configureProcess(ProcessBuilder builder) {
      processIds.add(builder.getProcessId());
      ExecutorParameters executorParams =
          ExecutorParameters.builder()
              .immediateRetries(0)
              .maximumRetries(0)
              .timeout(Duration.ofSeconds(10))
              .build();

      for (int i = 0; i < stageCnt; ++i) {
        builder
            .execute("STAGE" + i)
            .withCallExecutor(
                (request) -> {
                  stageExecCnt.incrementAndGet();
                  if (stageTestResult == StageTestResult.ERROR) {
                    return StageExecutorResult.error();
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

  private void saveSchedule(TestSchedule testSchedule) {
    ScheduleEntity schedule = new ScheduleEntity();
    schedule.setCron(testSchedule.cron);
    schedule.setServiceName(serviceConfiguration.getName());
    schedule.setPipelineName(testSchedule.pipelineName);
    scheduleService.saveSchedule(schedule);
    System.out.println("saved schedule for pipeline: " + testSchedule.pipelineName);
  }

  private void deleteSchedule(TestSchedule testSchedule) {
    ScheduleEntity schedule = new ScheduleEntity();
    schedule.setPipelineName(testSchedule.pipelineName);
    scheduleService.delete(schedule);
    System.out.println("deleted schedule for pipeline: " + testSchedule.pipelineName);
  }

  private void assertSchedulerMetrics(TestSchedule f) {
    String pipelineName = f.pipelineName();

    PipelineMetrics pipelineMetrics = metrics.pipeline(pipelineName);

    if (f.stageTestResult != StageTestResult.SUCCESS) {
      assertThat(pipelineMetrics.process().getFailedCount())
          .isEqualTo(f.stageExecCnt.get() / f.stageCnt);
      assertThat(pipelineMetrics.stage().getFailedCount()).isEqualTo(f.stageExecCnt.get());
      assertThat(pipelineMetrics.stage().getSuccessCount()).isEqualTo(0L);
      assertThat(TimeSeriesMetrics.getCount(pipelineMetrics.process().getFailedTimeSeries()))
          .isEqualTo(f.stageExecCnt.get() / f.stageCnt);
      assertThat(TimeSeriesMetrics.getCount(pipelineMetrics.stage().getFailedTimeSeries()))
          .isEqualTo(f.stageExecCnt.get());
      assertThat(TimeSeriesMetrics.getCount(pipelineMetrics.stage().getSuccessTimeSeries()))
          .isEqualTo(0);
    } else {
      assertThat(pipelineMetrics.process().getCompletedCount())
          .isEqualTo(f.stageExecCnt.get() / f.stageCnt);
      assertThat(pipelineMetrics.stage().getFailedCount()).isEqualTo(0L);
      assertThat(pipelineMetrics.stage().getSuccessCount()).isEqualTo(f.stageExecCnt.get());
      assertThat(TimeSeriesMetrics.getCount(pipelineMetrics.process().getCompletedTimeSeries()))
          .isEqualTo(f.stageExecCnt.get() / f.stageCnt);
      assertThat(TimeSeriesMetrics.getCount(pipelineMetrics.stage().getFailedTimeSeries()))
          .isEqualTo(0);
      assertThat(TimeSeriesMetrics.getCount(pipelineMetrics.stage().getSuccessTimeSeries()))
          .isEqualTo(f.stageExecCnt.get());
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
    assertThat(scheduleEntity.getCron()).isEqualTo(f.cron);
    assertThat(scheduleEntity.getStartTime()).isNotNull();
    assertThat(scheduleEntity.getEndTime()).isNotNull();
  }

  private void assertProcessEntity(TestSchedule f, String processId) {
    String pipelineName = f.pipelineName();

    ProcessEntity processEntity = processService.getSavedProcess(f.pipelineName(), processId).get();
    assertThat(processEntity.getPipelineName()).isEqualTo(pipelineName);
    assertThat(processEntity.getProcessId()).isEqualTo(processId);
    assertThat(processEntity.getExecutionCount()).isEqualTo(1);
    if (f.stageTestResult != StageTestResult.SUCCESS) {
      assertThat(processEntity.getProcessState())
          .isEqualTo(ProcessState.FAILED); // no re-executions allowed
    } else {
      assertThat(processEntity.getProcessState()).isEqualTo(ProcessState.COMPLETED);
    }
  }

  private void assertStageEntities(TestSchedule f, String processId) {
    String pipelineName = f.pipelineName();

    for (int i = 0; i < f.stageCnt; ++i) {
      StageEntity stageEntity =
          stageService.getSavedStage(f.pipelineName(), processId, "STAGE" + i).get();
      StageLogEntity stageLogEntity =
          stageService.getSavedStageLog(f.pipelineName(), processId, "STAGE" + i).get();
      assertThat(stageEntity.getPipelineName()).isEqualTo(pipelineName);
      assertThat(stageEntity.getProcessId()).isEqualTo(processId);
      assertThat(stageEntity.getExecutionCount()).isEqualTo(1);
      assertThat(stageEntity.getStartTime()).isNotNull();
      assertThat(stageEntity.getEndTime()).isNotNull();
      assertThat(stageEntity.getStartTime()).isBeforeOrEqualTo(stageEntity.getEndTime());
      assertThat(stageEntity.getExecutorName()).isEqualTo("pipelite.executor.CallExecutor");
      assertThat(stageEntity.getExecutorData()).isNull();
      assertThat(stageEntity.getExecutorParams())
          .isEqualTo(
              "{\n"
                  + "  \"timeout\" : 10000,\n"
                  + "  \"maximumRetries\" : 0,\n"
                  + "  \"immediateRetries\" : 0\n"
                  + "}");

      if (f.stageTestResult == StageTestResult.ERROR) {
        assertThat(stageEntity.getStageState()).isEqualTo(StageState.ERROR);
        assertThat(stageEntity.getResultParams()).isNull();
      } else if (f.stageTestResult == StageTestResult.EXCEPTION) {
        assertThat(stageEntity.getStageState()).isEqualTo(StageState.ERROR);
        assertThat(stageLogEntity.getStageLog())
            .contains(
                "pipelite.exception.PipeliteException: java.lang.RuntimeException: Expected exception");
      } else {
        assertThat(stageEntity.getStageState()).isEqualTo(StageState.SUCCESS);
        assertThat(stageEntity.getResultParams()).isNull();
      }
    }
  }

  private void test(List<TestSchedule> testProcessFactories) {
    PipeliteScheduler pipeliteScheduler = createPipeliteScheduler();
    try {
      for (TestSchedule f : testProcessFactories) {
        f.reset();
        saveSchedule(f);
        pipeliteScheduler.setMaximumExecutions(f.pipelineName(), f.processCnt);
      }
      new PipeliteServiceManager(serviceConfiguration, internalErrorService)
          .addService(pipeliteScheduler)
          .runSync();

      assertThat(pipeliteScheduler.getActiveProcessRunners().size()).isEqualTo(0);
      List<ScheduleEntity> scheduleEntities =
          scheduleService.getSchedules(serviceConfiguration.getName());
      for (TestSchedule f : testProcessFactories) {
        assertThat(f.stageExecCnt.get() / f.stageCnt).isEqualTo(f.processCnt);
        assertThat(f.processIds.size()).isEqualTo(f.processCnt);
        assertSchedulerMetrics(f);
        assertScheduleEntity(scheduleEntities, f);
        for (String processId : f.processIds) {
          assertProcessEntity(f, processId);
          assertStageEntities(f, processId);
        }
      }

    } finally {
      for (TestSchedule f : testProcessFactories) {
        deleteSchedule(f);
      }
    }
  }

  @Test
  public void testTwoSuccessSchedules() {
    test(Arrays.asList(firstProcessSuccess, secondProcessSuccess));
  }

  @Test
  public void testTwoFailureSchedules() {
    test(Arrays.asList(firstProcessFailure, secondProcessFailure));
  }

  @Test
  public void testTwoExceptionSchedules() {
    test(Arrays.asList(firstProcessException, secondProcessException));
  }

  @Test
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

  @Test
  public void testNextProcessId() {
    assertThat(PipeliteScheduler.nextProcessId(null)).isEqualTo("1");
    assertThat(PipeliteScheduler.nextProcessId("0")).isEqualTo("1");
    assertThat(PipeliteScheduler.nextProcessId("1")).isEqualTo("2");
    assertThat(PipeliteScheduler.nextProcessId("9")).isEqualTo("10");
    assertThat(PipeliteScheduler.nextProcessId("10")).isEqualTo("11");
    assertThat(PipeliteScheduler.nextProcessId("29")).isEqualTo("30");
    assertThat(PipeliteScheduler.nextProcessId("134232")).isEqualTo("134233");
  }

  @Test
  @Timeout(value = 10, unit = TimeUnit.SECONDS)
  public void testResumeSchedules() {
    int maxExecution1 = 1;
    int maxExecution2 = 1;

    // Create two schedules with start time and process id to allow processes to resume.

    ZonedDateTime startTime1 = ZonedDateTime.now().minusHours(1).truncatedTo(ChronoUnit.SECONDS);
    ZonedDateTime startTime2 = ZonedDateTime.now().minusHours(1).truncatedTo(ChronoUnit.SECONDS);

    ScheduleEntity scheduleEntity1 = scheduleService.getSavedSchedule(resume1.pipelineName()).get();
    ScheduleEntity scheduleEntity2 = scheduleService.getSavedSchedule(resume2.pipelineName()).get();
    scheduleEntity1.setStartTime(startTime1);
    scheduleEntity2.setStartTime(startTime2);
    String processId1 = "1";
    String processId2 = "2";
    scheduleEntity1.setProcessId(processId1);
    scheduleEntity2.setProcessId(processId2);
    scheduleService.saveSchedule(scheduleEntity1);
    scheduleService.saveSchedule(scheduleEntity2);

    ProcessEntity processEntity1 =
        ProcessEntity.createExecution(resume1.pipelineName(), processId1, 5);
    ProcessEntity processEntity2 =
        ProcessEntity.createExecution(resume2.pipelineName(), processId2, 5);
    processService.saveProcess(processEntity1);
    processService.saveProcess(processEntity2);

    PipeliteConfiguration pipeliteConfiguration =
        new PipeliteConfiguration(
            serviceConfiguration, advancedConfiguration, executorConfiguration, metrics);

    PipeliteScheduler pipeliteScheduler =
        spy(
            DefaultPipeliteScheduler.create(
                pipeliteConfiguration,
                pipeliteServices(),
                registeredPipelineService.getRegisteredPipelines(Schedule.class)));

    pipeliteScheduler.setMaximumExecutions(resume1.pipelineName(), maxExecution1);
    pipeliteScheduler.setMaximumExecutions(resume2.pipelineName(), maxExecution2);

    // Resume the two processes, check that they are immediately executed
    // and that they are scheduled for a later execution.

    pipeliteScheduler.startUp();

    while (pipeliteScheduler.getActiveProcessCount() > 0) {
      Time.wait(Duration.ofMillis(100));
    }

    scheduleEntity1 = scheduleService.getSavedSchedule(resume1.pipelineName()).get();
    scheduleEntity2 = scheduleService.getSavedSchedule(resume2.pipelineName()).get();
    assertThat(scheduleEntity1.getStartTime()).isEqualTo(startTime1);
    assertThat(scheduleEntity2.getStartTime()).isEqualTo(startTime2);
    assertThat(scheduleEntity1.getEndTime()).isAfter(startTime1);
    assertThat(scheduleEntity2.getEndTime()).isAfter(startTime2);
    assertThat(scheduleEntity1.getExecutionCount()).isOne();
    assertThat(scheduleEntity2.getExecutionCount()).isOne();
    assertThat(scheduleEntity1.getProcessId()).isEqualTo(processId1);
    assertThat(scheduleEntity2.getProcessId()).isEqualTo(processId2);
  }
}
