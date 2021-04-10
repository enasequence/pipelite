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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Profile;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.transaction.annotation.Transactional;
import pipelite.PipeliteTestConfigWithServices;
import pipelite.RegisteredPipeline;
import pipelite.UniqueStringGenerator;
import pipelite.entity.ProcessEntity;
import pipelite.entity.StageEntity;
import pipelite.exception.PipeliteRetryException;
import pipelite.helper.PipelineTestHelper;
import pipelite.process.Process;
import pipelite.process.ProcessFactory;
import pipelite.process.ProcessState;
import pipelite.process.builder.ProcessBuilder;
import pipelite.stage.Stage;
import pipelite.stage.StageState;
import pipelite.stage.executor.StageExecutorResult;
import pipelite.stage.parameters.ExecutorParameters;

@SpringBootTest(
    classes = PipeliteTestConfigWithServices.class,
    properties = {"pipelite.service.force=true", "pipelite.service.name=RetryServiceTest"})
@DirtiesContext
@ActiveProfiles({"test", "RetryServiceTest"})
@Transactional
class RetryServiceTest {

  private static final String PIPELINE_NAME =
      UniqueStringGenerator.randomPipelineName(RetryServiceTest.class);
  private static final String STAGE_NAME = "STAGE";

  @Autowired RegisteredPipelineService registeredPipelineService;
  @Autowired RetryService retryService;
  @Autowired ProcessService processService;
  @Autowired StageService stageService;

  @Profile("RetryServiceTest")
  @TestConfiguration
  static class TestConfig {
    @Bean("failedPipeline")
    public FailedPipeline failedPipeline() {
      return new FailedPipeline();
    }
  }

  public static class FailedPipeline extends PipelineTestHelper {
    public FailedPipeline() {
      super(PIPELINE_NAME, 5);
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

  @Test
  public void retryFailedProcess() {
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
    assertThat(processEntity.getPipelineName()).isEqualTo(PIPELINE_NAME);
    assertThat(processEntity.getProcessId()).isEqualTo(processId);
    assertThat(processEntity.getPriority()).isEqualTo(1);
    assertThat(processEntity.getExecutionCount()).isEqualTo(1);
    assertThat(processEntity.getStartTime()).isNotNull();
    assertThat(processEntity.getEndTime()).isNotNull();
    assertThat(processEntity.getProcessState()).isEqualTo(ProcessState.FAILED);

    // Save failed stage
    Stage stage = process.getStage(STAGE_NAME).get();
    stage.setStageEntity(stageService.createExecution(PIPELINE_NAME, processId, stage));
    stageService.startExecution(stage);
    stageService.endExecution(stage, StageExecutorResult.error());

    // Check failed stage
    StageEntity stageEntity =
        stageService.getSavedStage(PIPELINE_NAME, processId, STAGE_NAME).get();
    assertThat(stageEntity.getPipelineName()).isEqualTo(PIPELINE_NAME);
    assertThat(stageEntity.getProcessId()).isEqualTo(processId);
    assertThat(stageEntity.getStageName()).isEqualTo(STAGE_NAME);
    assertThat(stageEntity.getExecutionCount()).isEqualTo(1);
    assertThat(stageEntity.getStageState()).isEqualTo(StageState.ERROR);
    assertThat(stageEntity.getStartTime()).isNotNull();
    assertThat(stageEntity.getEndTime()).isNotNull();

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

    // Check stage state
    stageEntity = stageService.getSavedStage(PIPELINE_NAME, processId, STAGE_NAME).get();
    assertThat(stageEntity.getPipelineName()).isEqualTo(PIPELINE_NAME);
    assertThat(stageEntity.getProcessId()).isEqualTo(processId);
    assertThat(stageEntity.getStageName()).isEqualTo(STAGE_NAME);
    assertThat(stageEntity.getExecutionCount()).isEqualTo(0); // Made 0
    assertThat(stageEntity.getStageState()).isEqualTo(StageState.PENDING);
    assertThat(stageEntity.getStartTime()).isNull(); // Made null
    assertThat(stageEntity.getEndTime()).isNull(); // Made null
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
  public void retryFailedProcessThrowsBecauseNoPermanenentlyFailedStages() {
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
}
