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

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.transaction.annotation.Transactional;
import pipelite.PipeliteTestConfiguration;
import pipelite.UniqueStringGenerator;
import pipelite.entity.ProcessEntity;
import pipelite.entity.StageEntity;
import pipelite.exception.PipeliteProcessStateChangeException;
import pipelite.process.Process;
import pipelite.process.ProcessState;
import pipelite.process.builder.ProcessBuilder;
import pipelite.stage.Stage;
import pipelite.stage.StageState;
import pipelite.stage.executor.StageExecutorResult;
import pipelite.stage.parameters.ExecutorParameters;

@SpringBootTest(classes = PipeliteTestConfiguration.class)
@Transactional
@DirtiesContext
class ProcessServiceTest {

  private static final int DEFAULT_LIMIT = Integer.MAX_VALUE;

  @Autowired ProcessService service;
  @Autowired StageService stageService;

  @Test
  public void lifecycle() {

    String pipelineName = UniqueStringGenerator.randomPipelineName(this.getClass());
    String processId = UniqueStringGenerator.randomProcessId(this.getClass());
    int priority = 1;

    ProcessEntity processEntity = service.createExecution(pipelineName, processId, priority);

    assertThat(processEntity.getPipelineName()).isEqualTo(pipelineName);
    assertThat(processEntity.getProcessId()).isEqualTo(processId);
    assertThat(processEntity.getPriority()).isEqualTo(priority);
    assertThat(processEntity.getExecutionCount()).isEqualTo(0);
    assertThat(processEntity.getStartTime()).isNull();
    assertThat(processEntity.getEndTime()).isNull();
    assertThat(processEntity.getProcessState()).isEqualTo(ProcessState.PENDING);

    service.startExecution(processEntity);

    assertThat(processEntity.getPipelineName()).isEqualTo(pipelineName);
    assertThat(processEntity.getProcessId()).isEqualTo(processId);
    assertThat(processEntity.getPriority()).isEqualTo(priority);
    assertThat(processEntity.getExecutionCount()).isEqualTo(0);
    assertThat(processEntity.getStartTime()).isNotNull();
    assertThat(processEntity.getEndTime()).isNull();
    assertThat(processEntity.getProcessState()).isEqualTo(ProcessState.ACTIVE);

    assertThat(service.getSavedProcess(pipelineName, processId).get()).isEqualTo(processEntity);

    Process process = new ProcessBuilder(processId).execute("STAGE").withCallExecutor().build();
    process.setProcessEntity(processEntity);

    service.endExecution(process, ProcessState.COMPLETED);

    assertThat(processEntity.getPipelineName()).isEqualTo(pipelineName);
    assertThat(processEntity.getProcessId()).isEqualTo(processId);
    assertThat(processEntity.getPriority()).isEqualTo(priority);
    assertThat(processEntity.getExecutionCount()).isEqualTo(1);
    assertThat(processEntity.getStartTime()).isNotNull();
    assertThat(processEntity.getEndTime()).isNotNull();
    assertThat(processEntity.getProcessState()).isEqualTo(ProcessState.COMPLETED);

    assertThat(service.getSavedProcess(pipelineName, processId).get()).isEqualTo(processEntity);
  }

  @Test
  public void retryFailedProcess() {
    String pipelineName = UniqueStringGenerator.randomPipelineName(this.getClass());
    String processId = UniqueStringGenerator.randomProcessId(this.getClass());
    String stageName = UniqueStringGenerator.randomStageName(this.getClass());

    Process process =
        new ProcessBuilder(processId)
            .execute(stageName)
            .withCallExecutor(
                StageState.ERROR,
                ExecutorParameters.builder().maximumRetries(0).immediateRetries(0).build())
            .build();

    // Save failed process
    process.setProcessEntity(service.createExecution(pipelineName, processId, 1));
    service.startExecution(process.getProcessEntity());
    service.endExecution(process, ProcessState.FAILED);

    // Check failed process
    ProcessEntity processEntity = service.getSavedProcess(pipelineName, processId).get();
    assertThat(processEntity.getPipelineName()).isEqualTo(pipelineName);
    assertThat(processEntity.getProcessId()).isEqualTo(processId);
    assertThat(processEntity.getPriority()).isEqualTo(1);
    assertThat(processEntity.getExecutionCount()).isEqualTo(1);
    assertThat(processEntity.getStartTime()).isNotNull();
    assertThat(processEntity.getEndTime()).isNotNull();
    assertThat(processEntity.getProcessState()).isEqualTo(ProcessState.FAILED);

    // Save failed stage
    Stage stage = process.getStage(stageName).get();
    stage.setStageEntity(stageService.createExecution(pipelineName, processId, stage));
    stageService.startExecution(stage);
    stageService.endExecution(stage, StageExecutorResult.error());

    // Check failed stage
    StageEntity stageEntity = stageService.getSavedStage(pipelineName, processId, stageName).get();
    assertThat(stageEntity.getPipelineName()).isEqualTo(pipelineName);
    assertThat(stageEntity.getProcessId()).isEqualTo(processId);
    assertThat(stageEntity.getStageName()).isEqualTo(stageName);
    assertThat(stageEntity.getExecutionCount()).isEqualTo(1);
    assertThat(stageEntity.getStageState()).isEqualTo(StageState.ERROR);
    assertThat(stageEntity.getStartTime()).isNotNull();
    assertThat(stageEntity.getEndTime()).isNotNull();

    // Retry
    service.retry(pipelineName, process);

    processEntity = service.getSavedProcess(pipelineName, processId).get();
    assertThat(processEntity.getPipelineName()).isEqualTo(pipelineName);
    assertThat(processEntity.getProcessId()).isEqualTo(processId);
    assertThat(processEntity.getPriority()).isEqualTo(1);
    assertThat(processEntity.getExecutionCount()).isEqualTo(1);
    assertThat(processEntity.getStartTime()).isNotNull();
    assertThat(processEntity.getEndTime()).isNull(); // Made null
    assertThat(processEntity.getProcessState()).isEqualTo(ProcessState.ACTIVE);

    // Check stage state
    stageEntity = stageService.getSavedStage(pipelineName, processId, stageName).get();
    assertThat(stageEntity.getPipelineName()).isEqualTo(pipelineName);
    assertThat(stageEntity.getProcessId()).isEqualTo(processId);
    assertThat(stageEntity.getStageName()).isEqualTo(stageName);
    assertThat(stageEntity.getExecutionCount()).isEqualTo(0); // Made 0
    assertThat(stageEntity.getStageState()).isEqualTo(StageState.PENDING);
    assertThat(stageEntity.getStartTime()).isNull(); // Made null
    assertThat(stageEntity.getEndTime()).isNull(); // Made null
  }

  @Test
  public void retryFailedProcessThrowsBecauseNotFailed() {
    String pipelineName = UniqueStringGenerator.randomPipelineName(this.getClass());
    String processId = UniqueStringGenerator.randomProcessId(this.getClass());
    String stageName = UniqueStringGenerator.randomStageName(this.getClass());

    Process process =
        new ProcessBuilder(processId)
            .execute(stageName)
            .withCallExecutor(
                StageState.ERROR,
                ExecutorParameters.builder().maximumRetries(0).immediateRetries(0).build())
            .build();

    // Save completed process
    process.setProcessEntity(service.createExecution(pipelineName, processId, 1));
    service.startExecution(process.getProcessEntity());
    service.endExecution(process, ProcessState.COMPLETED);

    // Check completed process
    ProcessEntity processEntity = service.getSavedProcess(pipelineName, processId).get();
    assertThat(processEntity.getProcessState()).isEqualTo(ProcessState.COMPLETED);

    // Retry
    Exception exception =
        assertThrows(
            PipeliteProcessStateChangeException.class, () -> service.retry(pipelineName, process));
    assertThat(exception.getMessage()).contains("process is COMPLETED but should be FAILED");
  }

  @Test
  public void retryFailedProcessThrowsUnknownStage() {
    String pipelineName = UniqueStringGenerator.randomPipelineName(this.getClass());
    String processId = UniqueStringGenerator.randomProcessId(this.getClass());
    String stageName = UniqueStringGenerator.randomStageName(this.getClass());

    Process process =
        new ProcessBuilder(processId)
            .execute(stageName)
            .withCallExecutor(
                StageState.ERROR,
                ExecutorParameters.builder().maximumRetries(0).immediateRetries(0).build())
            .build();

    // Save completed process
    process.setProcessEntity(service.createExecution(pipelineName, processId, 1));
    service.startExecution(process.getProcessEntity());
    service.endExecution(process, ProcessState.FAILED);

    // Check completed process
    ProcessEntity processEntity = service.getSavedProcess(pipelineName, processId).get();
    assertThat(processEntity.getProcessState()).isEqualTo(ProcessState.FAILED);

    // Retry
    Exception exception =
        assertThrows(
            PipeliteProcessStateChangeException.class, () -> service.retry(pipelineName, process));
    assertThat(exception.getMessage()).contains("unknown stage");
  }

  @Test
  public void retryFailedProcessThrowsBecauseNoPermanenentlyFailedStages() {
    String pipelineName = UniqueStringGenerator.randomPipelineName(this.getClass());
    String processId = UniqueStringGenerator.randomProcessId(this.getClass());
    String stageName = UniqueStringGenerator.randomStageName(this.getClass());

    Process process =
        new ProcessBuilder(processId)
            .execute(stageName)
            .withCallExecutor(
                StageState.ERROR,
                ExecutorParameters.builder().maximumRetries(0).immediateRetries(0).build())
            .build();

    // Save failed process
    process.setProcessEntity(service.createExecution(pipelineName, processId, 1));
    service.startExecution(process.getProcessEntity());
    service.endExecution(process, ProcessState.FAILED);

    // Check failed process
    ProcessEntity processEntity = service.getSavedProcess(pipelineName, processId).get();
    assertThat(processEntity.getProcessState()).isEqualTo(ProcessState.FAILED);

    // Save completed stage
    Stage stage = process.getStage(stageName).get();
    stage.setStageEntity(stageService.createExecution(pipelineName, processId, stage));
    stageService.startExecution(stage);
    stageService.endExecution(stage, StageExecutorResult.success());

    // Check completed stage
    StageEntity stageEntity = stageService.getSavedStage(pipelineName, processId, stageName).get();
    assertThat(stageEntity.getStageState()).isEqualTo(StageState.SUCCESS);

    // Retry
    Exception exception =
        assertThrows(
            PipeliteProcessStateChangeException.class, () -> service.retry(pipelineName, process));
    assertThat(exception.getMessage()).contains("no stages to reset");
  }

  @Test
  public void rerunCompletedProcess() {
    String pipelineName = UniqueStringGenerator.randomPipelineName(this.getClass());
    String processId = UniqueStringGenerator.randomProcessId(this.getClass());
    String stageName1 = UniqueStringGenerator.randomStageName(this.getClass());
    String stageName2 = UniqueStringGenerator.randomStageName(this.getClass());
    String stageName3 = UniqueStringGenerator.randomStageName(this.getClass());

    // Stage 2 depends on stage 1. Stage 1 will be rerun and so will stage 2.

    Process process =
        new ProcessBuilder(processId)
            .execute(stageName1)
            .withCallExecutor(
                StageState.SUCCESS,
                ExecutorParameters.builder().maximumRetries(0).immediateRetries(0).build())
            .executeAfter(stageName2, stageName1)
            .withCallExecutor(
                StageState.SUCCESS,
                ExecutorParameters.builder().maximumRetries(0).immediateRetries(0).build())
            .execute(stageName3)
            .withCallExecutor(
                StageState.SUCCESS,
                ExecutorParameters.builder().maximumRetries(0).immediateRetries(0).build())
            .build();

    // Save completed process
    process.setProcessEntity(service.createExecution(pipelineName, processId, 1));
    service.startExecution(process.getProcessEntity());
    service.endExecution(process, ProcessState.COMPLETED);

    // Check completed process
    ProcessEntity processEntity = service.getSavedProcess(pipelineName, processId).get();
    assertThat(processEntity.getPipelineName()).isEqualTo(pipelineName);
    assertThat(processEntity.getProcessId()).isEqualTo(processId);
    assertThat(processEntity.getPriority()).isEqualTo(1);
    assertThat(processEntity.getExecutionCount()).isEqualTo(1);
    assertThat(processEntity.getStartTime()).isNotNull();
    assertThat(processEntity.getEndTime()).isNotNull();
    assertThat(processEntity.getProcessState()).isEqualTo(ProcessState.COMPLETED);

    // Save completed stages
    process
        .getStages()
        .forEach(
            stage -> {
              String stageName = stage.getStageName();
              stage = process.getStage(stageName).get();
              stage.setStageEntity(stageService.createExecution(pipelineName, processId, stage));
              stageService.startExecution(stage);
              stageService.endExecution(stage, StageExecutorResult.success());
            });

    process
        .getStages()
        .forEach(
            stage -> {
              String stageName = stage.getStageName();
              // Check completed stages
              StageEntity stageEntity =
                  stageService.getSavedStage(pipelineName, processId, stageName).get();
              assertThat(stageEntity.getPipelineName()).isEqualTo(pipelineName);
              assertThat(stageEntity.getProcessId()).isEqualTo(processId);
              assertThat(stageEntity.getStageName()).isEqualTo(stageName);
              assertThat(stageEntity.getExecutionCount()).isEqualTo(1);
              assertThat(stageEntity.getStageState()).isEqualTo(StageState.SUCCESS);
              assertThat(stageEntity.getStartTime()).isNotNull();
              assertThat(stageEntity.getEndTime()).isNotNull();
            });

    // Retry
    service.rerun(pipelineName, stageName1, process);

    processEntity = service.getSavedProcess(pipelineName, processId).get();
    assertThat(processEntity.getPipelineName()).isEqualTo(pipelineName);
    assertThat(processEntity.getProcessId()).isEqualTo(processId);
    assertThat(processEntity.getPriority()).isEqualTo(1);
    assertThat(processEntity.getExecutionCount()).isEqualTo(1);
    assertThat(processEntity.getStartTime()).isNotNull();
    assertThat(processEntity.getEndTime()).isNull(); // Made null
    assertThat(processEntity.getProcessState()).isEqualTo(ProcessState.ACTIVE);

    // Check stage state
    StageEntity stageEntity = stageService.getSavedStage(pipelineName, processId, stageName1).get();
    assertThat(stageEntity.getPipelineName()).isEqualTo(pipelineName);
    assertThat(stageEntity.getProcessId()).isEqualTo(processId);
    assertThat(stageEntity.getStageName()).isEqualTo(stageName1);
    assertThat(stageEntity.getExecutionCount()).isEqualTo(0); // Made 0
    assertThat(stageEntity.getStageState()).isEqualTo(StageState.PENDING);
    assertThat(stageEntity.getStartTime()).isNull(); // Made null
    assertThat(stageEntity.getEndTime()).isNull(); // Made null

    stageEntity = stageService.getSavedStage(pipelineName, processId, stageName2).get();
    assertThat(stageEntity.getPipelineName()).isEqualTo(pipelineName);
    assertThat(stageEntity.getProcessId()).isEqualTo(processId);
    assertThat(stageEntity.getStageName()).isEqualTo(stageName2);
    assertThat(stageEntity.getExecutionCount()).isEqualTo(0); // Made 0
    assertThat(stageEntity.getStageState()).isEqualTo(StageState.PENDING);
    assertThat(stageEntity.getStartTime()).isNull(); // Made null
    assertThat(stageEntity.getEndTime()).isNull(); // Made null

    stageEntity = stageService.getSavedStage(pipelineName, processId, stageName3).get();
    assertThat(stageEntity.getPipelineName()).isEqualTo(pipelineName);
    assertThat(stageEntity.getProcessId()).isEqualTo(processId);
    assertThat(stageEntity.getStageName()).isEqualTo(stageName3);
    assertThat(stageEntity.getExecutionCount()).isEqualTo(1);
    assertThat(stageEntity.getStageState()).isEqualTo(StageState.SUCCESS);
    assertThat(stageEntity.getStartTime()).isNotNull();
    assertThat(stageEntity.getEndTime()).isNotNull();
  }

  @Test
  public void rerunCompletedProcessThrowsBecauseNotCompleted() {
    String pipelineName = UniqueStringGenerator.randomPipelineName(this.getClass());
    String processId = UniqueStringGenerator.randomProcessId(this.getClass());
    String stageName = UniqueStringGenerator.randomStageName(this.getClass());

    Process process =
        new ProcessBuilder(processId)
            .execute(stageName)
            .withCallExecutor(
                StageState.ERROR,
                ExecutorParameters.builder().maximumRetries(0).immediateRetries(0).build())
            .build();

    // Save failed process
    process.setProcessEntity(service.createExecution(pipelineName, processId, 1));
    service.startExecution(process.getProcessEntity());
    service.endExecution(process, ProcessState.FAILED);

    // Check completed process
    ProcessEntity processEntity = service.getSavedProcess(pipelineName, processId).get();
    assertThat(processEntity.getProcessState()).isEqualTo(ProcessState.FAILED);

    // Retry
    Exception exception =
        assertThrows(
            PipeliteProcessStateChangeException.class,
            () -> service.rerun(pipelineName, stageName, process));
    assertThat(exception.getMessage()).contains("process is FAILED but should be COMPLETED");
  }

  @Test
  public void getActiveCompletedFailedPendingProcessesWithSamePriority() {
    String pipelineName = UniqueStringGenerator.randomPipelineName(this.getClass());

    saveProcess(pipelineName, ProcessState.ACTIVE, 1);
    saveProcess(pipelineName, ProcessState.ACTIVE, 1);
    saveProcess(pipelineName, ProcessState.COMPLETED, 1);
    saveProcess(pipelineName, ProcessState.COMPLETED, 1);
    saveProcess(pipelineName, ProcessState.COMPLETED, 1);
    saveProcess(pipelineName, ProcessState.FAILED, 1);
    saveProcess(pipelineName, ProcessState.FAILED, 1);
    saveProcess(pipelineName, ProcessState.FAILED, 1);
    saveProcess(pipelineName, ProcessState.FAILED, 1);
    saveProcess(pipelineName, ProcessState.PENDING, 1);
    saveProcess(pipelineName, ProcessState.PENDING, 1);

    assertThat(service.getAvailableActiveProcesses(pipelineName, DEFAULT_LIMIT)).hasSize(2);
    assertThat(service.getCompletedProcesses(pipelineName, DEFAULT_LIMIT)).hasSize(3);
    assertThat(service.getFailedProcesses(pipelineName, DEFAULT_LIMIT)).hasSize(4);
    assertThat(service.getPendingProcesses(pipelineName, DEFAULT_LIMIT)).hasSize(2);
  }

  @Test
  public void getActiveCompletedFailedPendingProcessesWithDifferentPriority() {
    String pipelineName = UniqueStringGenerator.randomPipelineName(this.getClass());

    saveProcess(pipelineName, ProcessState.ACTIVE, 1);
    saveProcess(pipelineName, ProcessState.ACTIVE, 2);
    saveProcess(pipelineName, ProcessState.COMPLETED, 1);
    saveProcess(pipelineName, ProcessState.COMPLETED, 2);
    saveProcess(pipelineName, ProcessState.COMPLETED, 3);
    saveProcess(pipelineName, ProcessState.FAILED, 1);
    saveProcess(pipelineName, ProcessState.FAILED, 2);
    saveProcess(pipelineName, ProcessState.FAILED, 3);
    saveProcess(pipelineName, ProcessState.FAILED, 4);
    saveProcess(pipelineName, ProcessState.PENDING, 1);
    saveProcess(pipelineName, ProcessState.PENDING, 2);

    assertThat(service.getAvailableActiveProcesses(pipelineName, DEFAULT_LIMIT)).hasSize(2);
    assertThat(service.getCompletedProcesses(pipelineName, DEFAULT_LIMIT)).hasSize(3);
    assertThat(service.getFailedProcesses(pipelineName, DEFAULT_LIMIT)).hasSize(4);
    assertThat(service.getPendingProcesses(pipelineName, DEFAULT_LIMIT)).hasSize(2);

    assertThat(service.getAvailableActiveProcesses(pipelineName, DEFAULT_LIMIT))
        .isSortedAccordingTo(Comparator.comparingInt(ProcessEntity::getPriority).reversed());
    assertThat(service.getFailedProcesses(pipelineName, DEFAULT_LIMIT))
        .isSortedAccordingTo(Comparator.comparingInt(ProcessEntity::getPriority).reversed());
    assertThat(service.getPendingProcesses(pipelineName, DEFAULT_LIMIT))
        .isSortedAccordingTo(Comparator.comparingInt(ProcessEntity::getPriority).reversed());
  }

  @Test
  public void getProcesses() {
    String pipelineName = UniqueStringGenerator.randomPipelineName(this.getClass());

    List<ProcessEntity> processes =
        Arrays.asList(
            saveProcess(pipelineName, ProcessState.ACTIVE, 1),
            saveProcess(pipelineName, ProcessState.ACTIVE, 1),
            saveProcess(pipelineName, ProcessState.COMPLETED, 1),
            saveProcess(pipelineName, ProcessState.COMPLETED, 1),
            saveProcess(pipelineName, ProcessState.FAILED, 1),
            saveProcess(pipelineName, ProcessState.FAILED, 1),
            saveProcess(pipelineName, ProcessState.PENDING, 1),
            saveProcess(pipelineName, ProcessState.PENDING, 1));

    // Test with pipeline name.

    testProcessStateCount(
        service.getProcesses(pipelineName, null /* state*/, DEFAULT_LIMIT), 2, 2, 2, 2);

    // Test with pipeline name and state.

    for (ProcessState state :
        EnumSet.of(
            ProcessState.ACTIVE,
            ProcessState.COMPLETED,
            ProcessState.FAILED,
            ProcessState.CANCELLED)) {
      testProcessStateCount(
          service.getProcesses(pipelineName, state, DEFAULT_LIMIT),
          state == ProcessState.COMPLETED ? 2 : 0,
          state == ProcessState.ACTIVE ? 2 : 0,
          state == ProcessState.FAILED ? 2 : 0,
          state == ProcessState.PENDING ? 2 : 0);
    }
  }

  private ProcessEntity saveProcess(String pipelineName, ProcessState state, int priority) {
    ProcessEntity processEntity =
        ProcessEntity.createExecution(
            pipelineName, UniqueStringGenerator.randomProcessId(this.getClass()), priority);
    processEntity.setProcessState(state);
    processEntity.setExecutionCount(0);
    return service.saveProcess(processEntity);
  }

  private void testProcessStateCount(
      List<ProcessEntity> processes,
      int completedCount,
      int activeCount,
      int failedCount,
      int pendingCount) {
    Map<ProcessState, AtomicInteger> counter = new HashMap<>();
    counter.put(ProcessState.COMPLETED, new AtomicInteger());
    counter.put(ProcessState.ACTIVE, new AtomicInteger());
    counter.put(ProcessState.FAILED, new AtomicInteger());
    counter.put(ProcessState.PENDING, new AtomicInteger());
    processes.forEach(process -> counter.get(process.getProcessState()).incrementAndGet());
    assertThat(counter.get(ProcessState.COMPLETED).get()).isEqualTo(completedCount);
    assertThat(counter.get(ProcessState.ACTIVE).get()).isEqualTo(activeCount);
    assertThat(counter.get(ProcessState.FAILED).get()).isEqualTo(failedCount);
    assertThat(counter.get(ProcessState.PENDING).get()).isEqualTo(pendingCount);
  }

  @Test
  public void testProcessStateSummary() {
    service.getProcessStateSummary();
  }
}
