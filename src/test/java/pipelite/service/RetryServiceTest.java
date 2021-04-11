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
package pipelite.service;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Profile;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.transaction.annotation.Transactional;
import pipelite.PipeliteTestConfigWithManager;
import pipelite.PipeliteTestConstants;
import pipelite.RegisteredPipeline;
import pipelite.UniqueStringGenerator;
import pipelite.cron.CronUtils;
import pipelite.entity.ProcessEntity;
import pipelite.entity.ScheduleEntity;
import pipelite.entity.StageEntity;
import pipelite.exception.PipeliteRetryException;
import pipelite.helper.PipelineTestHelper;
import pipelite.helper.ScheduleTestHelper;
import pipelite.manager.ProcessRunnerPoolManager;
import pipelite.process.Process;
import pipelite.process.ProcessFactory;
import pipelite.process.ProcessState;
import pipelite.process.builder.ProcessBuilder;
import pipelite.runner.schedule.ScheduleRunner;
import pipelite.stage.Stage;
import pipelite.stage.StageState;
import pipelite.stage.executor.StageExecutorResult;
import pipelite.stage.parameters.ExecutorParameters;
import pipelite.time.Time;

import java.time.Duration;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

@SpringBootTest(
    classes = PipeliteTestConfigWithManager.class,
    properties = {
      "pipelite.advanced.processRunnerFrequency=250ms",
      "pipelite.service.force=true",
      "pipelite.service.name=RetryServiceTest"
    })
@DirtiesContext
@ActiveProfiles({"test", "RetryServiceTest"})
@Transactional
class RetryServiceTest {

  private static final String PIPELINE_NAME =
      UniqueStringGenerator.randomPipelineName(RetryServiceTest.class);
  private static final String SCHEDULE_NAME =
      UniqueStringGenerator.randomPipelineName(RetryServiceTest.class);

  private static final String STAGE_NAME = "STAGE";

  @Autowired RegisteredPipelineService registeredPipelineService;
  @Autowired ProcessRunnerPoolManager processRunnerPoolManager;
  @Autowired RetryService retryService;
  @Autowired ScheduleService scheduleService;
  @Autowired ProcessService processService;
  @Autowired StageService stageService;
  @Autowired RunnerService runnerService;

  @Autowired TestSchedule testSchedule;

  @Profile("RetryServiceTest")
  @TestConfiguration
  static class TestConfig {
    @Bean
    public TestSchedule testSchedule() {
      return new TestSchedule();
    }

    @Bean
    public TestPipeline testPipeline() {
      return new TestPipeline();
    }
  }

  public static class TestSchedule extends ScheduleTestHelper {
    public TestSchedule() {
      super(SCHEDULE_NAME);
    }

    @Override
    protected String _configureCron() {
      return PipeliteTestConstants.CRON_EVERY_HOUR;
    }

    @Override
    protected void _configureProcess(ProcessBuilder builder) {
      builder.execute(STAGE_NAME).withCallExecutor().build();
    }
  }

  public static class TestPipeline extends PipelineTestHelper {
    public TestPipeline() {
      super(PIPELINE_NAME);
    }

    @Override
    public int _configureParallelism() {
      return 5;
    }

    @Override
    public void _configureProcess(ProcessBuilder builder) {
      builder
          .execute(STAGE_NAME)
          .withCallExecutor(
              StageState.ERROR,
              ExecutorParameters.builder().maximumRetries(0).immediateRetries(0).build())
          .build();
    }
  }

  @AfterEach
  public void clean() {
    ScheduleEntity scheduleEntity = new ScheduleEntity();
    scheduleEntity.setPipelineName(SCHEDULE_NAME);
    scheduleService.delete(scheduleEntity);
  }

  @Test
  public void retryFailedProcess() {
    String processId = UniqueStringGenerator.randomProcessId(this.getClass());
    RegisteredPipeline registeredPipeline =
        registeredPipelineService.getRegisteredPipeline(PIPELINE_NAME);
    Process process = ProcessFactory.create(processId, registeredPipeline);

    // Failed process
    process.setProcessEntity(processService.createExecution(PIPELINE_NAME, processId, 1));
    processService.startExecution(process.getProcessEntity());
    ProcessEntity processEntity = processService.endExecution(process, ProcessState.FAILED);

    // Failed stage
    Stage stage = process.getStage(STAGE_NAME).get();
    stage.setStageEntity(stageService.createExecution(PIPELINE_NAME, processId, stage));
    stageService.startExecution(stage);
    StageEntity stageEntity = stageService.endExecution(stage, StageExecutorResult.error());

    assertThat(processEntity.getProcessState()).isEqualTo(ProcessState.FAILED);
    assertThat(stageEntity.getStageState()).isEqualTo(StageState.ERROR);

    // Retry
    retryService.retry(PIPELINE_NAME, processId);

    // Check process state
    processEntity = processService.getSavedProcess(PIPELINE_NAME, processId).get();
    assertThat(processEntity.getPipelineName()).isEqualTo(PIPELINE_NAME);
    assertThat(processEntity.getProcessId()).isEqualTo(processId);
    assertThat(processEntity.getStartTime()).isNotNull();
    assertThat(processEntity.getEndTime()).isNull();
    assertThat(processEntity.getProcessState()).isEqualTo(ProcessState.ACTIVE);

    // Check stage state
    stageEntity = stageService.getSavedStage(PIPELINE_NAME, processId, STAGE_NAME).get();
    assertThat(stageEntity.getPipelineName()).isEqualTo(PIPELINE_NAME);
    assertThat(stageEntity.getProcessId()).isEqualTo(processId);
    assertThat(stageEntity.getStageName()).isEqualTo(STAGE_NAME);
    assertThat(stageEntity.getStageState()).isEqualTo(StageState.PENDING);
    assertThat(stageEntity.getStartTime()).isNull();
    assertThat(stageEntity.getEndTime()).isNull();
  }

  @Test
  public void retryFailedProcessThrowsBecauseNotFailed() {
    String processId = UniqueStringGenerator.randomProcessId(this.getClass());
    RegisteredPipeline registeredPipeline =
        registeredPipelineService.getRegisteredPipeline(PIPELINE_NAME);
    Process process = ProcessFactory.create(processId, registeredPipeline);

    // Save completed process
    process.setProcessEntity(processService.createExecution(PIPELINE_NAME, processId, 1));
    processService.startExecution(process.getProcessEntity());
    processService.endExecution(process, ProcessState.COMPLETED);

    // Check completed process
    ProcessEntity processEntity = processService.getSavedProcess(PIPELINE_NAME, processId).get();
    assertThat(processEntity.getProcessState()).isEqualTo(ProcessState.COMPLETED);

    // Retry
    Exception exception =
        assertThrows(
            PipeliteRetryException.class, () -> retryService.retry(PIPELINE_NAME, processId));
    assertThat(exception.getMessage()).contains("process is not failed");
  }

  @Test
  public void retryFailedProcessThrowsUnknownStage() {
    String processId = UniqueStringGenerator.randomProcessId(this.getClass());
    RegisteredPipeline registeredPipeline =
        registeredPipelineService.getRegisteredPipeline(PIPELINE_NAME);
    Process process = ProcessFactory.create(processId, registeredPipeline);

    // Save completed process
    process.setProcessEntity(processService.createExecution(PIPELINE_NAME, processId, 1));
    processService.startExecution(process.getProcessEntity());
    processService.endExecution(process, ProcessState.FAILED);

    // Check completed process
    ProcessEntity processEntity = processService.getSavedProcess(PIPELINE_NAME, processId).get();
    assertThat(processEntity.getProcessState()).isEqualTo(ProcessState.FAILED);

    // Retry
    Exception exception =
        assertThrows(
            PipeliteRetryException.class, () -> retryService.retry(PIPELINE_NAME, processId));
    assertThat(exception.getMessage()).contains("unknown stage");
  }

  @Test
  public void retryFailedProcessNoPermanentlyFailedStages() {
    String processId = UniqueStringGenerator.randomProcessId(this.getClass());
    RegisteredPipeline registeredPipeline =
        registeredPipelineService.getRegisteredPipeline(PIPELINE_NAME);
    Process process = ProcessFactory.create(processId, registeredPipeline);

    // Save failed process
    process.setProcessEntity(processService.createExecution(PIPELINE_NAME, processId, 1));
    processService.startExecution(process.getProcessEntity());
    processService.endExecution(process, ProcessState.FAILED);

    // Check failed process
    ProcessEntity processEntity = processService.getSavedProcess(PIPELINE_NAME, processId).get();
    assertThat(processEntity.getProcessState()).isEqualTo(ProcessState.FAILED);

    // Save completed stage
    Stage stage = process.getStage(STAGE_NAME).get();
    stage.setStageEntity(stageService.createExecution(PIPELINE_NAME, processId, stage));
    stageService.startExecution(stage);
    stageService.endExecution(stage, StageExecutorResult.success());

    // Check completed stage
    StageEntity stageEntity =
        stageService.getSavedStage(PIPELINE_NAME, processId, STAGE_NAME).get();
    assertThat(stageEntity.getStageState()).isEqualTo(StageState.SUCCESS);

    // Retry
    retryService.retry(PIPELINE_NAME, processId);

    processEntity = processService.getSavedProcess(PIPELINE_NAME, processId).get();
    assertThat(processEntity.getPipelineName()).isEqualTo(PIPELINE_NAME);
    assertThat(processEntity.getProcessId()).isEqualTo(processId);
    assertThat(processEntity.getPriority()).isEqualTo(1);
    assertThat(processEntity.getExecutionCount()).isEqualTo(1);
    assertThat(processEntity.getStartTime()).isNotNull();
    assertThat(processEntity.getEndTime()).isNull(); // Made null
    assertThat(processEntity.getProcessState()).isEqualTo(ProcessState.ACTIVE);

    // Check that stage state is unchanged
    stageEntity.equals(stageService.getSavedStage(PIPELINE_NAME, processId, STAGE_NAME).get());
  }

  @Test
  public void retryFailedSchedule() {
    String serviceName = UniqueStringGenerator.randomServiceName(ScheduleServiceTest.class);
    String processId = UniqueStringGenerator.randomProcessId(this.getClass());
    RegisteredPipeline registeredPipeline =
        registeredPipelineService.getRegisteredPipeline(SCHEDULE_NAME);
    Process process = ProcessFactory.create(processId, registeredPipeline);

    // Failed process
    process.setProcessEntity(processService.createExecution(SCHEDULE_NAME, processId, 1));
    processService.startExecution(process.getProcessEntity());
    ProcessEntity processEntity = processService.endExecution(process, ProcessState.FAILED);

    // Failed stage
    Stage stage = process.getStage(STAGE_NAME).get();
    stage.setStageEntity(stageService.createExecution(SCHEDULE_NAME, processId, stage));
    stageService.startExecution(stage);
    StageEntity stageEntity = stageService.endExecution(stage, StageExecutorResult.error());

    assertThat(processEntity.getProcessState()).isEqualTo(ProcessState.FAILED);
    assertThat(stageEntity.getStageState()).isEqualTo(StageState.ERROR);

    // Failed schedule
    String cron = testSchedule.cron();
    scheduleService.createSchedule(serviceName, SCHEDULE_NAME, cron);
    ScheduleEntity scheduleEntity = scheduleService.startExecution(SCHEDULE_NAME, processId);
    ZonedDateTime nextTime = CronUtils.launchTime(cron, ZonedDateTime.now().plusHours(1));
    scheduleEntity = scheduleService.endExecution(processEntity, nextTime);

    assertThat(scheduleEntity.isFailed()).isTrue();

    // Create schedule runner
    processRunnerPoolManager._createScheduleRunner();

    ScheduleRunner scheduleRunner = runnerService.getScheduleRunner();
    scheduleRunner.setMaximumExecutions(SCHEDULE_NAME, 1);
    assertThat(scheduleRunner.getScheduleCrons().size()).isOne();
    assertThat(scheduleRunner.getScheduleCrons().get(0).getPipelineName()).isEqualTo(SCHEDULE_NAME);

    scheduleRunner.startUp();

    // Check schedule state
    assertSetupSchedule(serviceName, SCHEDULE_NAME, processId, cron, nextTime);

    ZonedDateTime retryTime = ZonedDateTime.now();

    // Retry
    retryService.retry(SCHEDULE_NAME, processId);

    scheduleRunner.runOneIteration();

    while (scheduleRunner.getActiveProcessCount() > 0) {
      Time.wait(Duration.ofMillis(100));
    }

    // Check schedule state
    assertRetriedSchedule(serviceName, SCHEDULE_NAME, processId, cron, retryTime);

    // Check process state
    processEntity = processService.getSavedProcess(SCHEDULE_NAME, processId).get();
    assertThat(processEntity.getPipelineName()).isEqualTo(SCHEDULE_NAME);
    assertThat(processEntity.getProcessId()).isEqualTo(processId);
    assertThat(processEntity.getStartTime()).isNotNull();
    assertThat(processEntity.getEndTime()).isNotNull();
    assertThat(processEntity.getProcessState()).isEqualTo(ProcessState.COMPLETED);

    // Check stage state
    stageEntity = stageService.getSavedStage(SCHEDULE_NAME, processId, STAGE_NAME).get();
    assertThat(stageEntity.getPipelineName()).isEqualTo(SCHEDULE_NAME);
    assertThat(stageEntity.getProcessId()).isEqualTo(processId);
    assertThat(stageEntity.getStageName()).isEqualTo(STAGE_NAME);
    assertThat(stageEntity.getStageState()).isEqualTo(StageState.SUCCESS);
    assertThat(stageEntity.getStartTime()).isNotNull();
    assertThat(stageEntity.getEndTime()).isNotNull();
  }

  private void assertSetupSchedule(
      String serviceName,
      String pipelineName,
      String processId,
      String cron,
      ZonedDateTime nextTime) {
    // Failed
    ScheduleEntity s = scheduleService.getSavedSchedule(pipelineName).get();
    assertThat(s.getServiceName()).isEqualTo(serviceName);
    assertThat(s.getPipelineName()).isEqualTo(pipelineName);
    assertThat(s.getCron()).isEqualTo(cron);
    assertThat(s.getDescription()).isEqualTo(CronUtils.describe(cron));
    assertThat(s.getExecutionCount()).isEqualTo(1);
    assertThat(s.getNextTime()).isEqualTo(nextTime.truncatedTo(ChronoUnit.SECONDS));
    assertThat(s.getStartTime()).isNotNull();
    assertThat(s.getEndTime()).isNotNull();
    assertThat(s.getLastCompleted()).isNull();
    assertThat(s.getLastFailed()).isNotNull();
    assertThat(s.getStreakCompleted()).isEqualTo(0);
    assertThat(s.getStreakFailed()).isEqualTo(1);
    assertThat(s.getProcessId()).isEqualTo(processId);
    assertThat(s.isFailed()).isEqualTo(true);
    assertThat(s.isActive()).isEqualTo(false);
  }

  private void assertRetriedSchedule(
      String serviceName,
      String pipelineName,
      String processId,
      String cron,
      ZonedDateTime nextTime) {
    // Success
    ScheduleEntity s = scheduleService.getSavedSchedule(pipelineName).get();
    assertThat(s.getServiceName()).isEqualTo(serviceName);
    assertThat(s.getPipelineName()).isEqualTo(pipelineName);
    assertThat(s.getCron()).isEqualTo(cron);
    assertThat(s.getDescription()).isEqualTo(CronUtils.describe(cron));
    assertThat(s.getExecutionCount()).isEqualTo(2);
    assertThat(s.getNextTime()).isAfterOrEqualTo(nextTime.truncatedTo(ChronoUnit.SECONDS));
    assertThat(s.getStartTime()).isNotNull();
    assertThat(s.getEndTime()).isNotNull();
    assertThat(s.getLastCompleted()).isNotNull();
    assertThat(s.getLastFailed()).isNotNull();
    assertThat(s.getStreakCompleted()).isEqualTo(1);
    assertThat(s.getStreakFailed()).isEqualTo(0);
    assertThat(s.getProcessId()).isEqualTo(processId);
    assertThat(s.isFailed()).isEqualTo(false);
    assertThat(s.isActive()).isEqualTo(false);
  }
}
