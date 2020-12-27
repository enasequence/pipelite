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

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import pipelite.UniqueStringGenerator;
import pipelite.entity.ProcessEntity;
import pipelite.process.Process;
import pipelite.process.ProcessState;
import pipelite.process.builder.ProcessBuilder;

class ProcessServiceTester {

  private static final int DEFAULT_LIMIT = Integer.MAX_VALUE;

  public ProcessServiceTester(ProcessService service) {
    this.service = service;
  }

  private final ProcessService service;

  public void lifecycle() {

    String pipelineName = UniqueStringGenerator.randomPipelineName();
    String processId = UniqueStringGenerator.randomProcessId();
    int priority = 1;

    ProcessEntity processEntity = service.createExecution(pipelineName, processId, priority);

    assertThat(processEntity.getPipelineName()).isEqualTo(pipelineName);
    assertThat(processEntity.getProcessId()).isEqualTo(processId);
    assertThat(processEntity.getPriority()).isEqualTo(priority);
    assertThat(processEntity.getExecutionCount()).isEqualTo(0);
    assertThat(processEntity.getStartTime()).isNull();
    assertThat(processEntity.getEndTime()).isNull();
    assertThat(processEntity.getState()).isEqualTo(ProcessState.PENDING);

    service.startExecution(processEntity);

    assertThat(processEntity.getPipelineName()).isEqualTo(pipelineName);
    assertThat(processEntity.getProcessId()).isEqualTo(processId);
    assertThat(processEntity.getPriority()).isEqualTo(priority);
    assertThat(processEntity.getExecutionCount()).isEqualTo(0);
    assertThat(processEntity.getStartTime()).isNotNull();
    assertThat(processEntity.getEndTime()).isNull();
    assertThat(processEntity.getState()).isEqualTo(ProcessState.ACTIVE);

    assertThat(service.getSavedProcess(pipelineName, processId).get()).isEqualTo(processEntity);

    Process process =
        new ProcessBuilder(processId).execute("STAGE").withEmptySyncExecutor().build();
    process.setProcessEntity(processEntity);

    service.endExecution(process, ProcessState.COMPLETED);

    assertThat(processEntity.getPipelineName()).isEqualTo(pipelineName);
    assertThat(processEntity.getProcessId()).isEqualTo(processId);
    assertThat(processEntity.getPriority()).isEqualTo(priority);
    assertThat(processEntity.getExecutionCount()).isEqualTo(1);
    assertThat(processEntity.getStartTime()).isNotNull();
    assertThat(processEntity.getEndTime()).isNotNull();
    assertThat(processEntity.getState()).isEqualTo(ProcessState.COMPLETED);

    assertThat(service.getSavedProcess(pipelineName, processId).get()).isEqualTo(processEntity);
  }

  public void testGetActiveCompletedFailedPendingProcessesWithSamePriority() {
    String pipelineName = UniqueStringGenerator.randomPipelineName();

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

  public void testGetActiveCompletedFailedPendingProcessesWithDifferentPriority() {
    String pipelineName = UniqueStringGenerator.randomPipelineName();

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

  public void testGetProcesses() {
    String pipelineName = UniqueStringGenerator.randomPipelineName();

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
        service.getProcesses(pipelineName, null /* processId */, null /* state*/, DEFAULT_LIMIT),
        2,
        2,
        2,
        2);

    // Test with pipeline name and process id.

    for (ProcessEntity process : processes) {
      testProcessStateCount(
          service.getProcesses(
              pipelineName, process.getProcessId(), null /* state*/, DEFAULT_LIMIT),
          process.getState().equals(ProcessState.COMPLETED) ? 1 : 0,
          process.getState().equals(ProcessState.ACTIVE) ? 1 : 0,
          process.getState().equals(ProcessState.FAILED) ? 1 : 0,
          process.getState().equals(ProcessState.PENDING) ? 1 : 0);
    }

    // Test with pipeline name and state.

    for (ProcessState state :
        EnumSet.of(
            ProcessState.ACTIVE,
            ProcessState.COMPLETED,
            ProcessState.FAILED,
            ProcessState.CANCELLED)) {
      testProcessStateCount(
          service.getProcesses(pipelineName, null /* processId */, state, DEFAULT_LIMIT),
          state == ProcessState.COMPLETED ? 2 : 0,
          state == ProcessState.ACTIVE ? 2 : 0,
          state == ProcessState.FAILED ? 2 : 0,
          state == ProcessState.PENDING ? 2 : 0);
    }

    // Test with pipeline name, process id and state.

    for (ProcessEntity process : processes) {
      testProcessStateCount(
          service.getProcesses(
              pipelineName, process.getProcessId(), process.getState(), DEFAULT_LIMIT),
          process.getState().equals(ProcessState.COMPLETED) ? 1 : 0,
          process.getState().equals(ProcessState.ACTIVE) ? 1 : 0,
          process.getState().equals(ProcessState.FAILED) ? 1 : 0,
          process.getState().equals(ProcessState.PENDING) ? 1 : 0);
    }

    // Test with process id.

    for (ProcessEntity process : processes) {
      testProcessStateCount(
          service.getProcesses(null, process.getProcessId(), null /* state*/, DEFAULT_LIMIT),
          process.getState().equals(ProcessState.COMPLETED) ? 1 : 0,
          process.getState().equals(ProcessState.ACTIVE) ? 1 : 0,
          process.getState().equals(ProcessState.FAILED) ? 1 : 0,
          process.getState().equals(ProcessState.PENDING) ? 1 : 0);
    }

    // Test with process id and state.

    for (ProcessEntity process : processes) {
      testProcessStateCount(
          service.getProcesses(null, process.getProcessId(), process.getState(), DEFAULT_LIMIT),
          process.getState().equals(ProcessState.COMPLETED) ? 1 : 0,
          process.getState().equals(ProcessState.ACTIVE) ? 1 : 0,
          process.getState().equals(ProcessState.FAILED) ? 1 : 0,
          process.getState().equals(ProcessState.PENDING) ? 1 : 0);
    }
  }

  private ProcessEntity saveProcess(String pipelineName, ProcessState state, int priority) {
    ProcessEntity processEntity =
        ProcessEntity.createExecution(
            pipelineName, UniqueStringGenerator.randomProcessId(), priority);
    processEntity.setState(state);
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
    processes.forEach(
        process -> {
          counter.get(process.getState()).incrementAndGet();
        });
    assertThat(counter.get(ProcessState.COMPLETED).get()).isEqualTo(completedCount);
    assertThat(counter.get(ProcessState.ACTIVE).get()).isEqualTo(activeCount);
    assertThat(counter.get(ProcessState.FAILED).get()).isEqualTo(failedCount);
    assertThat(counter.get(ProcessState.PENDING).get()).isEqualTo(pendingCount);
  }
}
