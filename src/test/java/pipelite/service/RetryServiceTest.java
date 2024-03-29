/*
 * Copyright 2020-2022 EMBL - European Bioinformatics Institute
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package pipelite.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Duration;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
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
import pipelite.*;
import pipelite.cron.CronUtils;
import pipelite.entity.ProcessEntity;
import pipelite.entity.ScheduleEntity;
import pipelite.entity.StageEntity;
import pipelite.entity.field.StageState;
import pipelite.exception.PipeliteProcessRetryException;
import pipelite.manager.ProcessRunnerPoolManager;
import pipelite.process.Process;
import pipelite.process.ProcessFactory;
import pipelite.process.ProcessState;
import pipelite.process.builder.ProcessBuilder;
import pipelite.runner.schedule.ScheduleRunner;
import pipelite.stage.Stage;
import pipelite.stage.executor.StageExecutorResult;
import pipelite.stage.executor.StageExecutorState;
import pipelite.stage.parameters.ExecutorParameters;
import pipelite.test.PipeliteTestConstants;
import pipelite.test.PipeliteTestIdCreator;
import pipelite.test.configuration.PipeliteTestConfigWithManager;
import pipelite.time.Time;

@SpringBootTest(
    classes = PipeliteTestConfigWithManager.class,
    properties = {
      "pipelite.advanced.processRunnerFrequency=250ms",
      "pipelite.service.force=true",
      "pipelite.service.name=RetryServiceTest"
    })
@DirtiesContext
@ActiveProfiles({"pipelite", "RetryServiceTest"})
@Transactional
class RetryServiceTest {

  private static final String PIPELINE_NAME = PipeliteTestIdCreator.pipelineName();
  private static final String SCHEDULE_NAME = PipeliteTestIdCreator.pipelineName();
  private static final String STAGE_NAME = "STAGE";
  private static final String CRON = PipeliteTestConstants.CRON_EVERY_HOUR;

  @Autowired RegisteredPipelineService registeredPipelineService;
  @Autowired ProcessRunnerPoolManager processRunnerPoolManager;
  @Autowired RetryService retryService;
  @Autowired ScheduleService scheduleService;
  @Autowired ProcessService processService;
  @Autowired StageService stageService;
  @Autowired RunnerService runnerService;

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

  public static class TestSchedule implements Schedule {
    @Override
    public String pipelineName() {
      return SCHEDULE_NAME;
    }

    @Override
    public void configureProcess(ProcessBuilder builder) {
      builder.execute(STAGE_NAME).withSyncTestExecutor().build();
    }

    @Override
    public Options configurePipeline() {
      return new Options().cron(CRON);
    }
  }

  public static class TestPipeline implements Pipeline {
    @Override
    public String pipelineName() {
      return PIPELINE_NAME;
    }

    @Override
    public Options configurePipeline() {
      return new Options().pipelineParallelism(5);
    }

    @Override
    public void configureProcess(ProcessBuilder builder) {
      builder
          .execute(STAGE_NAME)
          .withSyncTestExecutor(
              StageExecutorState.EXECUTION_ERROR,
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
    String processId = PipeliteTestIdCreator.processId();
    ProcessEntity processEntity = new ProcessEntity();
    processEntity.setPipelineName(PIPELINE_NAME);
    processEntity.setProcessId(processId);
    RegisteredPipeline registeredPipeline =
        registeredPipelineService.getRegisteredPipeline(PIPELINE_NAME);
    Process process = ProcessFactory.create(processEntity, registeredPipeline);

    // Failed process
    process.setProcessEntity(processService.createExecution(PIPELINE_NAME, processId, 1));
    processService.startExecution(process.getProcessEntity());
    processEntity = processService.endExecution(process, ProcessState.FAILED);

    // Failed stage
    Stage stage = process.getStage(STAGE_NAME).get();
    stageService.prepareExecution(PIPELINE_NAME, processId, stage);
    stageService.startExecution(stage);
    stageService.endExecution(stage, StageExecutorResult.executionError());
    StageEntity stageEntity = stage.getStageEntity();

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
    String processId = PipeliteTestIdCreator.processId();
    ProcessEntity processEntity = new ProcessEntity();
    processEntity.setPipelineName(PIPELINE_NAME);
    processEntity.setProcessId(processId);
    RegisteredPipeline registeredPipeline =
        registeredPipelineService.getRegisteredPipeline(PIPELINE_NAME);
    Process process = ProcessFactory.create(processEntity, registeredPipeline);

    // Save completed process
    process.setProcessEntity(processService.createExecution(PIPELINE_NAME, processId, 1));
    processService.startExecution(process.getProcessEntity());
    processService.endExecution(process, ProcessState.COMPLETED);

    // Check completed process
    processEntity = processService.getSavedProcess(PIPELINE_NAME, processId).get();
    assertThat(processEntity.getProcessState()).isEqualTo(ProcessState.COMPLETED);

    // Retry
    Exception exception =
        assertThrows(
            PipeliteProcessRetryException.class,
            () -> retryService.retry(PIPELINE_NAME, processId));
    assertThat(exception.getMessage()).contains("process is not failed");
  }

  @Test
  public void retryFailedProcessThrowsUnknownStage() {
    String processId = PipeliteTestIdCreator.processId();
    ProcessEntity processEntity = new ProcessEntity();
    processEntity.setPipelineName(PIPELINE_NAME);
    processEntity.setProcessId(processId);
    RegisteredPipeline registeredPipeline =
        registeredPipelineService.getRegisteredPipeline(PIPELINE_NAME);
    Process process = ProcessFactory.create(processEntity, registeredPipeline);

    // Save completed process
    process.setProcessEntity(processService.createExecution(PIPELINE_NAME, processId, 1));
    processService.startExecution(process.getProcessEntity());
    processService.endExecution(process, ProcessState.FAILED);

    // Check completed process
    processEntity = processService.getSavedProcess(PIPELINE_NAME, processId).get();
    assertThat(processEntity.getProcessState()).isEqualTo(ProcessState.FAILED);

    // Retry
    Exception exception =
        assertThrows(
            PipeliteProcessRetryException.class,
            () -> retryService.retry(PIPELINE_NAME, processId));
    assertThat(exception.getMessage()).contains("unknown stage");
  }

  @Test
  public void retryFailedProcessNoPermanentlyFailedStages() {
    String processId = PipeliteTestIdCreator.processId();
    ProcessEntity processEntity = new ProcessEntity();
    processEntity.setPipelineName(PIPELINE_NAME);
    processEntity.setProcessId(processId);
    RegisteredPipeline registeredPipeline =
        registeredPipelineService.getRegisteredPipeline(PIPELINE_NAME);
    Process process = ProcessFactory.create(processEntity, registeredPipeline);

    // Save failed process
    process.setProcessEntity(processService.createExecution(PIPELINE_NAME, processId, 1));
    processService.startExecution(process.getProcessEntity());
    processService.endExecution(process, ProcessState.FAILED);

    // Check failed process
    processEntity = processService.getSavedProcess(PIPELINE_NAME, processId).get();
    assertThat(processEntity.getProcessState()).isEqualTo(ProcessState.FAILED);

    // Save completed stage
    Stage stage = process.getStage(STAGE_NAME).get();
    stageService.prepareExecution(PIPELINE_NAME, processId, stage);
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
    String serviceName = PipeliteTestIdCreator.serviceName();
    String processId = PipeliteTestIdCreator.processId();
    ProcessEntity processEntity = new ProcessEntity();
    processEntity.setPipelineName(SCHEDULE_NAME);
    processEntity.setProcessId(processId);
    RegisteredPipeline registeredPipeline =
        registeredPipelineService.getRegisteredPipeline(SCHEDULE_NAME);
    Process process = ProcessFactory.create(processEntity, registeredPipeline);

    // Failed process
    process.setProcessEntity(processService.createExecution(SCHEDULE_NAME, processId, 1));
    processService.startExecution(process.getProcessEntity());
    processEntity = processService.endExecution(process, ProcessState.FAILED);

    // Failed stage
    Stage stage = process.getStage(STAGE_NAME).get();
    stageService.prepareExecution(SCHEDULE_NAME, processId, stage);
    stageService.startExecution(stage);
    stageService.endExecution(stage, StageExecutorResult.executionError());
    StageEntity stageEntity = stage.getStageEntity();

    assertThat(processEntity.getProcessState()).isEqualTo(ProcessState.FAILED);
    assertThat(stageEntity.getStageState()).isEqualTo(StageState.ERROR);

    // Failed schedule
    scheduleService.createSchedule(serviceName, SCHEDULE_NAME, CRON);
    ScheduleEntity scheduleEntity = scheduleService.startExecution(SCHEDULE_NAME, processId);
    ZonedDateTime nextTime = CronUtils.launchTime(CRON, ZonedDateTime.now().plusHours(1));
    scheduleEntity = scheduleService.endExecution(processEntity, nextTime);

    assertThat(scheduleEntity.isFailed()).isTrue();

    // Create schedule runner
    processRunnerPoolManager._createScheduleRunner();

    ScheduleRunner scheduleRunner = runnerService.getScheduleRunner();
    scheduleRunner.setIdleExecutions(SCHEDULE_NAME, 1);
    assertThat(scheduleRunner.getScheduleCrons().size()).isOne();
    assertThat(scheduleRunner.getScheduleCrons().get(0).getPipelineName()).isEqualTo(SCHEDULE_NAME);

    // Check schedule state
    assertSetupSchedule(serviceName, SCHEDULE_NAME, processId, CRON, nextTime);

    ZonedDateTime retryTime = ZonedDateTime.now();

    Time.wait(Duration.ofMillis(1000));
    scheduleRunner.startUp();

    // Retry
    retryService.retry(SCHEDULE_NAME, processId);

    while (!scheduleRunner.isIdle()) {
      scheduleRunner.runOneIteration();
      Time.wait(Duration.ofMillis(100));
    }

    // Check schedule state
    assertRetriedSchedule(serviceName, SCHEDULE_NAME, processId, CRON, retryTime);

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
