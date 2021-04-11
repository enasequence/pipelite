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
import org.springframework.test.context.ActiveProfiles;
import org.springframework.transaction.annotation.Transactional;
import pipelite.PipeliteTestConfigWithServices;
import pipelite.UniqueStringGenerator;
import pipelite.entity.ProcessEntity;
import pipelite.exception.PipeliteRetryException;
import pipelite.process.Process;
import pipelite.process.ProcessState;
import pipelite.process.builder.ProcessBuilder;

@SpringBootTest(
    classes = PipeliteTestConfigWithServices.class,
    properties = {"pipelite.service.force=true", "pipelite.service.name=ProcessServiceTest"})
@DirtiesContext
@ActiveProfiles("test")
@Transactional
class ProcessServiceTest {

  private static final int MAX_PROCESS_COUNT = Integer.MAX_VALUE;

  @Autowired ProcessService processService;
  @Autowired StageService stageService;

  @Test
  public void lifecycle() {

    String pipelineName = UniqueStringGenerator.randomPipelineName(this.getClass());
    String processId = UniqueStringGenerator.randomProcessId(this.getClass());
    int priority = 1;

    ProcessEntity processEntity = processService.createExecution(pipelineName, processId, priority);

    assertThat(processEntity.getPipelineName()).isEqualTo(pipelineName);
    assertThat(processEntity.getProcessId()).isEqualTo(processId);
    assertThat(processEntity.getPriority()).isEqualTo(priority);
    assertThat(processEntity.getExecutionCount()).isEqualTo(0);
    assertThat(processEntity.getStartTime()).isNull();
    assertThat(processEntity.getEndTime()).isNull();
    assertThat(processEntity.getProcessState()).isEqualTo(ProcessState.PENDING);

    processService.startExecution(processEntity);

    assertThat(processEntity.getPipelineName()).isEqualTo(pipelineName);
    assertThat(processEntity.getProcessId()).isEqualTo(processId);
    assertThat(processEntity.getPriority()).isEqualTo(priority);
    assertThat(processEntity.getExecutionCount()).isEqualTo(0);
    assertThat(processEntity.getStartTime()).isNotNull();
    assertThat(processEntity.getEndTime()).isNull();
    assertThat(processEntity.getProcessState()).isEqualTo(ProcessState.ACTIVE);

    assertThat(processService.getSavedProcess(pipelineName, processId).get())
        .isEqualTo(processEntity);

    Process process = new ProcessBuilder(processId).execute("STAGE").withCallExecutor().build();
    process.setProcessEntity(processEntity);

    processService.endExecution(process, ProcessState.COMPLETED);

    assertThat(processEntity.getPipelineName()).isEqualTo(pipelineName);
    assertThat(processEntity.getProcessId()).isEqualTo(processId);
    assertThat(processEntity.getPriority()).isEqualTo(priority);
    assertThat(processEntity.getExecutionCount()).isEqualTo(1);
    assertThat(processEntity.getStartTime()).isNotNull();
    assertThat(processEntity.getEndTime()).isNotNull();
    assertThat(processEntity.getProcessState()).isEqualTo(ProcessState.COMPLETED);

    assertThat(processService.getSavedProcess(pipelineName, processId).get())
        .isEqualTo(processEntity);
  }

  @Test
  public void getUnlockedActiveCompletedFailedPendingProcessesWithSamePriority() {
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

    assertThat(processService.getUnlockedActiveProcesses(pipelineName, MAX_PROCESS_COUNT))
        .hasSize(2);
    assertThat(processService.getCompletedProcesses(pipelineName, MAX_PROCESS_COUNT)).hasSize(3);
    assertThat(processService.getFailedProcesses(pipelineName, MAX_PROCESS_COUNT)).hasSize(4);
    assertThat(processService.getPendingProcesses(pipelineName, MAX_PROCESS_COUNT)).hasSize(2);
  }

  @Test
  public void getUnlockedActiveCompletedFailedPendingProcessesWithDifferentPriority() {
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

    assertThat(processService.getUnlockedActiveProcesses(pipelineName, MAX_PROCESS_COUNT))
        .hasSize(2);
    assertThat(processService.getCompletedProcesses(pipelineName, MAX_PROCESS_COUNT)).hasSize(3);
    assertThat(processService.getFailedProcesses(pipelineName, MAX_PROCESS_COUNT)).hasSize(4);
    assertThat(processService.getPendingProcesses(pipelineName, MAX_PROCESS_COUNT)).hasSize(2);

    assertThat(processService.getUnlockedActiveProcesses(pipelineName, MAX_PROCESS_COUNT))
        .isSortedAccordingTo(Comparator.comparingInt(ProcessEntity::getPriority).reversed());
    assertThat(processService.getFailedProcesses(pipelineName, MAX_PROCESS_COUNT))
        .isSortedAccordingTo(Comparator.comparingInt(ProcessEntity::getPriority).reversed());
    assertThat(processService.getPendingProcesses(pipelineName, MAX_PROCESS_COUNT))
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

    // Test without state.

    assertProcessStateCount(
        processService.getProcesses(pipelineName, null /* state*/, MAX_PROCESS_COUNT), 2, 2, 2, 2);

    // Test with state.

    for (ProcessState state :
        EnumSet.of(
            ProcessState.ACTIVE,
            ProcessState.COMPLETED,
            ProcessState.FAILED,
            ProcessState.CANCELLED)) {
      assertProcessStateCount(
          processService.getProcesses(pipelineName, state, MAX_PROCESS_COUNT),
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
    return processService.saveProcess(processEntity);
  }

  private void assertProcessStateCount(
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
  public void getProcessStateSummary() {
    processService.getProcessStateSummary();
  }

  @Test
  public void isRetryProcessWithFailedProcess() {
    String pipelineName = UniqueStringGenerator.randomPipelineName(this.getClass());
    String processId = UniqueStringGenerator.randomProcessId(this.getClass());
    int priority = 1;

    ProcessEntity processEntity = processService.createExecution(pipelineName, processId, priority);
    processEntity.endExecution(ProcessState.FAILED);
    processService.saveProcess(processEntity);

    assertThat(processEntity.getProcessState()).isEqualTo(ProcessState.FAILED);
    assertThat(processService.isRetryProcess(pipelineName, processId)).isTrue();
  }

  @Test
  public void isRetryProcessWithNotFailedProcess() {
    String pipelineName = UniqueStringGenerator.randomPipelineName(this.getClass());
    String processId = UniqueStringGenerator.randomProcessId(this.getClass());
    int priority = 1;

    ProcessEntity processEntity = processService.createExecution(pipelineName, processId, priority);
    processEntity.endExecution(ProcessState.COMPLETED);
    processService.saveProcess(processEntity);

    assertThat(processEntity.getProcessState()).isEqualTo(ProcessState.COMPLETED);
    assertThrows(
        PipeliteRetryException.class, () -> processService.isRetryProcess(pipelineName, processId));
  }

  @Test
  public void isRetryProcessWithMissingProcess() {
    String pipelineName = UniqueStringGenerator.randomPipelineName(this.getClass());
    String processId = UniqueStringGenerator.randomProcessId(this.getClass());

    assertThat(processService.getSavedProcess(pipelineName, processId).isPresent()).isFalse();
    assertThrows(
        PipeliteRetryException.class, () -> processService.isRetryProcess(pipelineName, processId));
  }
}
