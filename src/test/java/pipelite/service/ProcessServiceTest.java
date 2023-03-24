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

import java.time.ZonedDateTime;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.transaction.annotation.Transactional;
import pipelite.entity.ProcessEntity;
import pipelite.exception.PipeliteProcessRetryException;
import pipelite.process.Process;
import pipelite.process.ProcessState;
import pipelite.process.builder.ProcessBuilder;
import pipelite.runner.process.ProcessQueuePriorityPolicy;
import pipelite.test.PipeliteTestIdCreator;
import pipelite.test.configuration.PipeliteTestConfigWithServices;

@SpringBootTest(
    classes = PipeliteTestConfigWithServices.class,
    properties = {
      "pipelite.service.force=true",
      "pipelite.service.name=ProcessServiceTest",
      "pipelite.advanced.processQueuePriorityPolicy=PRIORITY"
    })
@DirtiesContext
@Transactional
class ProcessServiceTest {

  private static final int MAX_PROCESS_COUNT = 1000;

  @Autowired ProcessService processService;
  @Autowired StageService stageService;

  @Test
  public void lifecycle() {

    String pipelineName = PipeliteTestIdCreator.pipelineName();
    String processId = PipeliteTestIdCreator.processId();
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

    Process process = new ProcessBuilder(processId).execute("STAGE").withSyncTestExecutor().build();
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
    String pipelineName = PipeliteTestIdCreator.pipelineName();

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
    String pipelineName = PipeliteTestIdCreator.pipelineName();

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
    String pipelineName = PipeliteTestIdCreator.pipelineName();

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
        ProcessEntity.createExecution(pipelineName, PipeliteTestIdCreator.processId(), priority);
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
    String pipelineName = PipeliteTestIdCreator.pipelineName();
    String processId = PipeliteTestIdCreator.processId();
    int priority = 1;

    ProcessEntity processEntity = processService.createExecution(pipelineName, processId, priority);
    processEntity.endExecution(ProcessState.FAILED);
    processService.saveProcess(processEntity);

    assertThat(processEntity.getProcessState()).isEqualTo(ProcessState.FAILED);
    assertThat(processService.isRetryProcess(pipelineName, processId)).isTrue();
  }

  @Test
  public void isRetryProcessWithNotFailedProcess() {
    String pipelineName = PipeliteTestIdCreator.pipelineName();
    String processId = PipeliteTestIdCreator.processId();
    int priority = 1;

    ProcessEntity processEntity = processService.createExecution(pipelineName, processId, priority);
    processEntity.endExecution(ProcessState.COMPLETED);
    processService.saveProcess(processEntity);

    assertThat(processEntity.getProcessState()).isEqualTo(ProcessState.COMPLETED);
    assertThrows(
        PipeliteProcessRetryException.class,
        () -> processService.isRetryProcess(pipelineName, processId));
  }

  @Test
  public void isRetryProcessWithMissingProcess() {
    String pipelineName = PipeliteTestIdCreator.pipelineName();
    String processId = PipeliteTestIdCreator.processId();

    assertThat(processService.getSavedProcess(pipelineName, processId).isPresent()).isFalse();
    assertThrows(
        PipeliteProcessRetryException.class,
        () -> processService.isRetryProcess(pipelineName, processId));
  }

  private ProcessEntity createProcessEntity(Integer priority, ZonedDateTime initTime) {
    ProcessEntity processEntity = new ProcessEntity();
    processEntity.setProcessId(PipeliteTestIdCreator.processId());
    processEntity.setPriority(priority);
    processEntity.setCreateTime(initTime);
    return processEntity;
  }

  @Test
  public void testPendingProcesses() {

    ProcessEntity p1 = createProcessEntity(1, ZonedDateTime.now().minusDays(5));
    ProcessEntity p2 = createProcessEntity(2, ZonedDateTime.now().minusDays(4));
    ProcessEntity p3 = createProcessEntity(3, ZonedDateTime.now().minusDays(3));
    ProcessEntity p4 = createProcessEntity(4, ZonedDateTime.now().minusDays(2));
    ProcessEntity p5 = createProcessEntity(5, ZonedDateTime.now().minusDays(1));

    List<ProcessEntity> fifoProcesses = Arrays.asList(p1, p2, p3, p4, p5);
    List<ProcessEntity> priorityProcesses = Arrays.asList(p5, p4, p3, p2, p1);

    // Test lower max process count
    assertThat(
            ProcessService.getPendingProcesses(
                ProcessQueuePriorityPolicy.PRIORITY, 2, fifoProcesses, priorityProcesses))
        .containsExactly(p5, p4);

    assertThat(
            ProcessService.getPendingProcesses(
                ProcessQueuePriorityPolicy.FIFO, 2, fifoProcesses, priorityProcesses))
        .containsExactly(p1, p2);

    // Test higher max process count
    assertThat(
            ProcessService.getPendingProcesses(
                ProcessQueuePriorityPolicy.PRIORITY, 50, fifoProcesses, priorityProcesses))
        .containsExactly(p5, p4, p3, p2, p1);

    assertThat(
            ProcessService.getPendingProcesses(
                ProcessQueuePriorityPolicy.FIFO, 50, fifoProcesses, priorityProcesses))
        .containsExactly(p1, p2, p3, p4, p5);

    // Test prefer priority
    assertThat(
            ProcessService.getPendingProcesses(
                ProcessQueuePriorityPolicy.PREFER_PRIORITY, 5, fifoProcesses, priorityProcesses))
        .containsExactly(p1, p5, p4, p3, p2);

    // Test prefer fifo
    assertThat(
            ProcessService.getPendingProcesses(
                ProcessQueuePriorityPolicy.PREFER_FIFO, 5, fifoProcesses, priorityProcesses))
        .containsExactly(p1, p2, p3, p5, p4);

    // Test balanced
    assertThat(
            ProcessService.getPendingProcesses(
                ProcessQueuePriorityPolicy.BALANCED, 5, fifoProcesses, priorityProcesses))
        .containsExactly(p1, p2, p5, p4, p3);
  }
}
