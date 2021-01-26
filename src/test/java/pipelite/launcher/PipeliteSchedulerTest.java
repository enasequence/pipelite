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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.times;
import static org.springframework.test.annotation.DirtiesContext.ClassMode.BEFORE_EACH_TEST_METHOD;

import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import lombok.Value;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.util.TestPropertyValues;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import pipelite.*;
import pipelite.configuration.AdvancedConfiguration;
import pipelite.configuration.ExecutorConfiguration;
import pipelite.configuration.ServiceConfiguration;
import pipelite.entity.ProcessEntity;
import pipelite.entity.ScheduleEntity;
import pipelite.entity.StageEntity;
import pipelite.entity.StageLogEntity;
import pipelite.launcher.process.runner.ProcessRunnerCallback;
import pipelite.launcher.process.runner.ProcessRunnerPool;
import pipelite.launcher.process.runner.ProcessRunnerResult;
import pipelite.lock.PipeliteLocker;
import pipelite.metrics.PipelineMetrics;
import pipelite.metrics.PipeliteMetrics;
import pipelite.metrics.TimeSeriesMetrics;
import pipelite.process.Process;
import pipelite.process.ProcessState;
import pipelite.process.builder.ProcessBuilder;
import pipelite.service.*;
import pipelite.stage.StageState;
import pipelite.stage.executor.StageExecutorResult;
import pipelite.stage.parameters.ExecutorParameters;
import pipelite.time.Time;

@SpringBootTest(
    webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
    classes = PipeliteTestConfiguration.class,
    properties = {"pipelite.advanced.processRunnerFrequency=250ms"})
@ContextConfiguration(initializers = PipeliteSchedulerTest.TestContextInitializer.class)
@DirtiesContext(classMode = BEFORE_EACH_TEST_METHOD)
public class PipeliteSchedulerTest {

  public static class TestContextInitializer
      implements ApplicationContextInitializer<ConfigurableApplicationContext> {
    @Override
    public void initialize(ConfigurableApplicationContext configurableApplicationContext) {
      TestPropertyValues.of("pipelite.service.name=" + UniqueStringGenerator.randomServiceName())
          .applyTo(configurableApplicationContext.getEnvironment());
    }
  }

  @Autowired private ServiceConfiguration serviceConfiguration;
  @Autowired private AdvancedConfiguration advancedConfiguration;
  @Autowired private ExecutorConfiguration executorConfiguration;
  @Autowired private RegisteredPipelineService registeredPipelineService;
  @Autowired private ScheduleService scheduleService;
  @Autowired private ProcessService processService;
  @Autowired private StageService stageService;
  @Autowired private PipeliteLockerService pipeliteLockerService;
  @Autowired private MailService mailService;

  @Autowired private TestPipeline firstProcessSuccess;
  @Autowired private TestPipeline secondProcessSuccess;
  @Autowired private TestPipeline firstProcessFailure;
  @Autowired private TestPipeline secondProcessFailure;
  @Autowired private TestPipeline firstProcessException;
  @Autowired private TestPipeline secondProcessException;
  @Autowired private PipeliteMetrics metrics;

  @TestConfiguration
  static class TestConfig {
    @Bean
    public Pipeline firstProcessSuccess() {
      return new TestPipeline("firstProcessSuccess", 2, 1, 2, StageTestResult.SUCCESS);
    }

    @Bean
    public Pipeline secondProcessSuccess() {
      return new TestPipeline("secondProcessSuccess", 1, 2, 4, StageTestResult.SUCCESS);
    }

    @Bean
    public Pipeline firstProcessFailure() {
      return new TestPipeline("firstProcessFailure", 2, 1, 2, StageTestResult.ERROR);
    }

    @Bean
    public Pipeline secondProcessFailure() {
      return new TestPipeline("secondProcessFailure", 1, 2, 4, StageTestResult.ERROR);
    }

    @Bean
    public Pipeline firstProcessException() {
      return new TestPipeline("firstProcessException", 2, 1, 2, StageTestResult.EXCEPTION);
    }

    @Bean
    public Pipeline secondProcessException() {
      return new TestPipeline("secondProcessException", 1, 2, 4, StageTestResult.EXCEPTION);
    }
  }

  private enum StageTestResult {
    SUCCESS,
    ERROR,
    EXCEPTION
  }

  private PipeliteScheduler createPipeliteScheduler() {
    return DefaultPipeliteScheduler.create(
        serviceConfiguration,
        advancedConfiguration,
        executorConfiguration,
        pipeliteLockerService.getPipeliteLocker(),
        registeredPipelineService,
        processService,
        scheduleService,
        stageService,
        mailService,
        metrics);
  }

  @Value
  public static class TestPipeline implements Pipeline {
    private final String pipelineName;
    public final int processCnt;
    public final int stageCnt;
    public final int schedulerSeconds; // 60 must be divisible by schedulerSeconds.
    public final StageTestResult stageTestResult;
    public final String cron;
    public final List<String> processIds = Collections.synchronizedList(new ArrayList<>());
    public final AtomicLong stageExecCnt = new AtomicLong();

    public TestPipeline(
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
      this.cron = "0/" + schedulerSeconds + " * * * * ?";
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
    public int getPipelineParallelism() {
      return 1;
    }

    @Override
    public Process createProcess(ProcessBuilder builder) {
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
      return builder.build();
    }
  }

  private void saveSchedule(TestPipeline testProcessFactory) {
    ScheduleEntity schedule = new ScheduleEntity();
    schedule.setCron(testProcessFactory.cron);
    schedule.setServiceName(serviceConfiguration.getName());
    schedule.setPipelineName(testProcessFactory.pipelineName);
    scheduleService.saveSchedule(schedule);
    System.out.println("saved schedule for pipeline: " + testProcessFactory.pipelineName);
  }

  private void deleteSchedule(TestPipeline testProcessFactory) {
    ScheduleEntity schedule = new ScheduleEntity();
    schedule.setPipelineName(testProcessFactory.pipelineName);
    scheduleService.delete(schedule);
    System.out.println("deleted schedule for pipeline: " + testProcessFactory.pipelineName);
  }

  private void assertSchedulerMetrics(TestPipeline f) {
    String pipelineName = f.getPipelineName();

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

  private void assertScheduleEntity(List<ScheduleEntity> scheduleEntities, TestPipeline f) {
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
    assertThat(scheduleEntity.getServiceName()).isEqualTo(serviceConfiguration.getName());
    assertThat(scheduleEntity.getPipelineName()).isEqualTo(pipelineName);
    assertThat(scheduleEntity.getProcessId()).isNotNull();
    assertThat(scheduleEntity.getExecutionCount()).isEqualTo(f.processCnt);
    assertThat(scheduleEntity.getCron()).isEqualTo(f.cron);
    assertThat(scheduleEntity.getStartTime()).isNotNull();
    assertThat(scheduleEntity.getEndTime()).isNotNull();
    assertThat(scheduleEntity.getDescription())
        .isEqualTo("every " + f.schedulerSeconds + " seconds");
  }

  private void assertProcessEntity(TestPipeline f, String processId) {
    String pipelineName = f.getPipelineName();

    ProcessEntity processEntity =
        processService.getSavedProcess(f.getPipelineName(), processId).get();
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

  private void assertStageEntities(TestPipeline f, String processId) {
    String pipelineName = f.getPipelineName();

    for (int i = 0; i < f.stageCnt; ++i) {
      StageEntity stageEntity =
          stageService.getSavedStage(f.getPipelineName(), processId, "STAGE" + i).get();
      StageLogEntity stageLogEntity =
          stageService.getSavedStageLog(f.getPipelineName(), processId, "STAGE" + i).get();
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

  private void test(List<TestPipeline> testProcessFactories) {
    PipeliteScheduler pipeliteScheduler = createPipeliteScheduler();
    try {
      for (TestPipeline f : testProcessFactories) {
        f.reset();
        saveSchedule(f);
        pipeliteScheduler.setMaximumExecutions(f.getPipelineName(), f.processCnt);
      }
      new PipeliteServiceManager().addService(pipeliteScheduler).runSync();

      assertThat(pipeliteScheduler.getActiveProcessRunners().size()).isEqualTo(0);
      List<ScheduleEntity> scheduleEntities =
          scheduleService.getSchedules(serviceConfiguration.getName());
      for (TestPipeline f : testProcessFactories) {
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
      for (TestPipeline f : testProcessFactories) {
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

  private static Process testProcess(String processId) {
    return new ProcessBuilder(processId).execute("STAGE").withCallExecutor().build();
  }

  @Test
  public void testInvalidCron() {
    String pipelineName1 = UniqueStringGenerator.randomPipelineName();

    // Create launcher configuration with schedule refresh frequency.

    AdvancedConfiguration advancedConfiguration = new AdvancedConfiguration();

    // Create schedule that has invalid cron.

    ScheduleEntity scheduleEntity1 = new ScheduleEntity();
    scheduleEntity1.setPipelineName(pipelineName1);
    String cron1 = "invalid";
    scheduleEntity1.setCron(cron1);

    PipeliteLocker pipeliteLocker = mock(PipeliteLocker.class);
    RegisteredPipelineService registeredPipelineService = mock(RegisteredPipelineService.class);
    ScheduleService scheduleService = mock(ScheduleService.class);
    ProcessService processService = mock(ProcessService.class);
    ProcessRunnerPool processRunnerPool = mock(ProcessRunnerPool.class);

    // Return schedule from the schedule service.

    doReturn(Arrays.asList(scheduleEntity1)).when(scheduleService).getSchedules(any());

    // Create pipelite scheduler.

    PipeliteMetrics metrics = PipeliteTestBeans.pipeliteMetrics();

    PipeliteScheduler pipeliteScheduler =
        spy(
            new PipeliteScheduler(
                serviceConfiguration,
                advancedConfiguration,
                pipeliteLocker,
                registeredPipelineService,
                scheduleService,
                processService,
                processRunnerPool,
                metrics));

    int maxExecution1 = 1;
    pipeliteScheduler.setMaximumExecutions(pipelineName1, maxExecution1);

    pipeliteScheduler.startUp();

    while (pipeliteScheduler.getActiveProcessCount() > 0) {
      Time.wait(Duration.ofMillis(100));
    }

    verify(processRunnerPool, times(0)).runProcess(any(), any(), any());

    assertThat(pipeliteScheduler.getSchedules().size()).isEqualTo(1);
    assertThat(pipeliteScheduler.getSchedules().get(0).getCron()).isEqualTo(cron1);
    assertThat(pipeliteScheduler.getSchedules().get(0).getLaunchTime()).isNull();
  }

  @Test
  public void testRefreshSchedules() {
    String pipelineName1 = UniqueStringGenerator.randomPipelineName();
    String pipelineName2 = UniqueStringGenerator.randomPipelineName();
    int maxExecution1 = 5;
    int maxExecution2 = 5;

    // Create launcher configuration with schedule refresh frequency.

    Duration scheduleRefreshFrequency = Duration.ofSeconds(5);
    AdvancedConfiguration advancedConfiguration = new AdvancedConfiguration();
    advancedConfiguration.setScheduleRefreshFrequency(scheduleRefreshFrequency);

    // Create two schedules.

    ScheduleEntity scheduleEntity1 = new ScheduleEntity();
    ScheduleEntity scheduleEntity2 = new ScheduleEntity();
    scheduleEntity1.setPipelineName(pipelineName1);
    scheduleEntity2.setPipelineName(pipelineName2);

    String cron1 = "0/2 * * * * ?"; // every two seconds
    String cron2 = "0/1 * * * * ?"; // every second
    scheduleEntity1.setCron(cron1);
    scheduleEntity2.setCron(cron2);

    PipeliteLocker pipeliteLocker = mock(PipeliteLocker.class);
    RegisteredPipelineService registeredPipelineService = mock(RegisteredPipelineService.class);
    ScheduleService scheduleService = mock(ScheduleService.class);

    Pipeline pipeline1 = new pipelite.TestPipeline(pipelineName1, Arrays.asList(testProcess("1")));
    Pipeline pipeline2 = new pipelite.TestPipeline(pipelineName2, Arrays.asList(testProcess("1")));
    doAnswer(I -> pipeline1).when(registeredPipelineService).getPipeline(eq(pipelineName1));
    doAnswer(I -> pipeline2).when(registeredPipelineService).getPipeline(eq(pipelineName2));

    // Return schedules from the schedule service.

    doReturn(Arrays.asList(scheduleEntity1, scheduleEntity2))
        .when(scheduleService)
        .getSchedules(any());
    doReturn(Optional.of(scheduleEntity1))
        .when(scheduleService)
        .getSavedSchedule(eq(pipelineName1));
    doReturn(Optional.of(scheduleEntity2))
        .when(scheduleService)
        .getSavedSchedule(eq(pipelineName2));

    // Create process service to create process entities.

    ProcessService processService = mock(ProcessService.class);
    doAnswer(
            I -> {
              ProcessEntity processEntity = new ProcessEntity();
              processEntity.setPipelineName(I.getArgument(0));
              processEntity.setProcessId(I.getArgument(1));
              return processEntity;
            })
        .when(processService)
        .createExecution(any(), any(), any());

    // Create process launcher pool.

    ProcessRunnerPool processRunnerPool = mock(ProcessRunnerPool.class);
    doAnswer(
            I -> {
              Process process = I.getArgument(1);
              ProcessRunnerCallback callback = I.getArgument(2);
              callback.accept(process, mock(ProcessRunnerResult.class));
              return null;
            })
        .when(processRunnerPool)
        .runProcess(any(), any(), any());

    // Create pipelite scheduler.

    PipeliteMetrics metrics = PipeliteTestBeans.pipeliteMetrics();

    PipeliteScheduler pipeliteScheduler =
        spy(
            new PipeliteScheduler(
                serviceConfiguration,
                advancedConfiguration,
                pipeliteLocker,
                registeredPipelineService,
                scheduleService,
                processService,
                processRunnerPool,
                metrics));

    pipeliteScheduler.setMaximumExecutions(pipelineName1, maxExecution1);
    pipeliteScheduler.setMaximumExecutions(pipelineName2, maxExecution2);

    // Check that there are no schedules yet and that new schedules can be created.

    assertThat(pipeliteScheduler.isRefreshSchedules()).isTrue();

    // Create new schedules. The schedules are not immediately executable. The schedules are not
    // allowed to be immediately refreshed.

    pipeliteScheduler.startUp();

    assertThat(pipeliteScheduler.isRefreshSchedules()).isFalse();

    // Wait for the two schedules to be allowed to be refreshed.

    Time.wait(scheduleRefreshFrequency.plusMillis(1));

    assertThat(pipeliteScheduler.isRefreshSchedules()).isTrue();

    ZonedDateTime launchTime1 = pipeliteScheduler.getSchedules().get(0).getLaunchTime();
    ZonedDateTime launchTime2 = pipeliteScheduler.getSchedules().get(1).getLaunchTime();

    // Check that no processes have been executed yet.

    verify(processRunnerPool, times(0)).runProcess(any(), any(), any());

    // Refresh the schedules and check that the launch times do not change. The schedules are not
    // allowed to be immediately refreshed.

    pipeliteScheduler.refreshSchedules();

    assertThat(pipeliteScheduler.isRefreshSchedules()).isFalse();

    assertThat(launchTime1).isEqualTo(pipeliteScheduler.getSchedules().get(0).getLaunchTime());
    assertThat(launchTime2).isEqualTo(pipeliteScheduler.getSchedules().get(1).getLaunchTime());

    // Run the scheduler and check that the launch times have been updated. The schedules are not be
    // immediately executable or pending.

    pipeliteScheduler.run();

    while (pipeliteScheduler.getActiveProcessCount() > 0) {
      Time.wait(Duration.ofMillis(100));
    }

    verify(pipeliteScheduler, times(1)).executeSchedules();
    verify(pipeliteScheduler, times(2)).executeSchedule(any(), any());
    verify(processRunnerPool, times(1)).runProcess(eq(pipelineName1), any(), any());
    verify(processRunnerPool, times(1)).runProcess(eq(pipelineName2), any(), any());

    assertThat(launchTime1).isBefore(pipeliteScheduler.getSchedules().get(0).getLaunchTime());
    assertThat(launchTime2).isBefore(pipeliteScheduler.getSchedules().get(1).getLaunchTime());
  }

  @Test
  public void testResumeSchedules() {
    String pipelineName1 = UniqueStringGenerator.randomPipelineName();
    String pipelineName2 = UniqueStringGenerator.randomPipelineName();
    int maxExecution1 = 5;
    int maxExecution2 = 5;

    // Create launcher configuration with schedule refresh frequency.

    Duration scheduleRefreshFrequency = Duration.ofSeconds(5);
    AdvancedConfiguration advancedConfiguration = new AdvancedConfiguration();
    advancedConfiguration.setScheduleRefreshFrequency(scheduleRefreshFrequency);

    // Create two schedules with start time and process id to allow processes to resume.

    ZonedDateTime launchTime1 = ZonedDateTime.now().minusHours(1);
    ZonedDateTime launchTime2 = ZonedDateTime.now().minusHours(1);

    ScheduleEntity scheduleEntity1 = new ScheduleEntity();
    ScheduleEntity scheduleEntity2 = new ScheduleEntity();
    scheduleEntity1.setPipelineName(pipelineName1);
    scheduleEntity2.setPipelineName(pipelineName2);
    scheduleEntity1.setStartTime(launchTime1);
    scheduleEntity2.setStartTime(launchTime2);
    String processId1 = "1";
    String processId2 = "2";
    scheduleEntity1.setProcessId(processId1);
    scheduleEntity2.setProcessId(processId2);

    String cron1 = "0/2 * * * * ?"; // every two seconds
    String cron2 = "0/1 * * * * ?"; // every second
    scheduleEntity1.setCron(cron1);
    scheduleEntity2.setCron(cron2);

    PipeliteLocker pipeliteLocker = mock(PipeliteLocker.class);
    RegisteredPipelineService registeredPipelineService = mock(RegisteredPipelineService.class);
    ScheduleService scheduleService = mock(ScheduleService.class);

    Pipeline pipeline1 =
        new pipelite.TestPipeline(pipelineName1, Arrays.asList(testProcess(processId1)));
    Pipeline pipeline2 =
        new pipelite.TestPipeline(pipelineName2, Arrays.asList(testProcess(processId2)));
    doAnswer(I -> pipeline1).when(registeredPipelineService).getPipeline(eq(pipelineName1));
    doAnswer(I -> pipeline2).when(registeredPipelineService).getPipeline(eq(pipelineName2));

    // Return schedules from the schedule service.

    doReturn(Arrays.asList(scheduleEntity1, scheduleEntity2))
        .when(scheduleService)
        .getSchedules(any());
    doReturn(Optional.of(scheduleEntity1))
        .when(scheduleService)
        .getSavedSchedule(eq(pipelineName1));
    doReturn(Optional.of(scheduleEntity2))
        .when(scheduleService)
        .getSavedSchedule(eq(pipelineName2));

    // Create process service to return saved process entities.

    ProcessService processService = mock(ProcessService.class);
    doAnswer(
            I -> {
              ProcessEntity processEntity = new ProcessEntity();
              processEntity.setPipelineName(I.getArgument(0));
              processEntity.setProcessId(I.getArgument(1));
              return Optional.of(processEntity);
            })
        .when(processService)
        .getSavedProcess(any(), any());

    // Create process launcher pool.

    ProcessRunnerPool processRunnerPool = mock(ProcessRunnerPool.class);
    doAnswer(
            I -> {
              Process process = I.getArgument(1);
              ProcessRunnerCallback callback = I.getArgument(2);
              callback.accept(process, mock(ProcessRunnerResult.class));
              return null;
            })
        .when(processRunnerPool)
        .runProcess(any(), any(), any());

    // Create pipelite scheduler.

    PipeliteMetrics metrics = PipeliteTestBeans.pipeliteMetrics();

    PipeliteScheduler pipeliteScheduler =
        spy(
            new PipeliteScheduler(
                serviceConfiguration,
                advancedConfiguration,
                pipeliteLocker,
                registeredPipelineService,
                scheduleService,
                processService,
                processRunnerPool,
                metrics));
    pipeliteScheduler.setMaximumExecutions(pipelineName1, maxExecution1);
    pipeliteScheduler.setMaximumExecutions(pipelineName2, maxExecution2);

    // Check that there are no schedules yet and that new schedules can be created.

    assertThat(pipeliteScheduler.isRefreshSchedules()).isTrue();

    // Resume the two processes, check that they are immediately executed
    // and that they are scheduled for a later execution.

    ZonedDateTime now = ZonedDateTime.now();

    pipeliteScheduler.startUp();

    while (pipeliteScheduler.getActiveProcessCount() > 0) {
      Time.wait(Duration.ofMillis(100));
    }

    verify(pipeliteScheduler, times(1)).resumeSchedules();
    verify(pipeliteScheduler, times(2)).resumeSchedule(any());
    verify(pipeliteScheduler, times(2)).executeSchedule(any(), any());
    verify(processRunnerPool, times(1)).runProcess(eq(pipelineName1), any(), any());
    verify(processRunnerPool, times(1)).runProcess(eq(pipelineName2), any(), any());

    assertThat(pipeliteScheduler.getSchedules().size()).isEqualTo(2);
    assertThat(pipeliteScheduler.getSchedules().get(0).getCron()).isEqualTo(cron1);
    assertThat(pipeliteScheduler.getSchedules().get(1).getCron()).isEqualTo(cron2);
    assertThat(pipeliteScheduler.getSchedules().get(0).getLaunchTime()).isAfter(now);
    assertThat(pipeliteScheduler.getSchedules().get(1).getLaunchTime()).isAfter(now);
  }
}
