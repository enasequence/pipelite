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
import pipelite.process.ProcessState;

class ProcessServiceTester {

  private static final int DEFAULT_LIMIT = Integer.MAX_VALUE;

  public ProcessServiceTester(ProcessService service) {
    this.service = service;
  }

  private final ProcessService service;

  public void testCrud() {

    String pipelineName = UniqueStringGenerator.randomPipelineName();
    String processId = UniqueStringGenerator.randomProcessId();
    ProcessState state = ProcessState.ACTIVE;
    Integer execCnt = 3;
    Integer priority = 0;

    ProcessEntity process = ProcessEntity.createExecution(pipelineName, processId, priority);
    process.setState(state);
    process.setExecutionCount(execCnt);

    service.saveProcess(process);

    assertThat(service.getSavedProcess(pipelineName, processId).get()).isEqualTo(process);

    process.setState(ProcessState.COMPLETED);
    process.setExecutionCount(4);
    process.setPriority(9);

    service.saveProcess(process);

    assertThat(service.getSavedProcess(pipelineName, processId).get()).isEqualTo(process);

    service.delete(process);

    assertThat(service.getSavedProcess(pipelineName, processId).isPresent()).isFalse();
  }

  public void testGetActiveCompletedFailedPendingSamePriority() {
    String pipelineName = UniqueStringGenerator.randomPipelineName();
    String launcherName = UniqueStringGenerator.randomLauncherName();

    assertThat(service.saveProcess(createProcess(pipelineName, ProcessState.ACTIVE, 1)).getState())
        .isEqualTo(ProcessState.ACTIVE);
    assertThat(service.saveProcess(createProcess(pipelineName, ProcessState.ACTIVE, 1)).getState())
        .isEqualTo(ProcessState.ACTIVE);
    assertThat(
            service.saveProcess(createProcess(pipelineName, ProcessState.COMPLETED, 1)).getState())
        .isEqualTo(ProcessState.COMPLETED);
    assertThat(
            service.saveProcess(createProcess(pipelineName, ProcessState.COMPLETED, 1)).getState())
        .isEqualTo(ProcessState.COMPLETED);
    assertThat(
            service.saveProcess(createProcess(pipelineName, ProcessState.COMPLETED, 1)).getState())
        .isEqualTo(ProcessState.COMPLETED);
    assertThat(service.saveProcess(createProcess(pipelineName, ProcessState.FAILED, 1)).getState())
        .isEqualTo(ProcessState.FAILED);
    assertThat(service.saveProcess(createProcess(pipelineName, ProcessState.FAILED, 1)).getState())
        .isEqualTo(ProcessState.FAILED);
    assertThat(service.saveProcess(createProcess(pipelineName, ProcessState.FAILED, 1)).getState())
        .isEqualTo(ProcessState.FAILED);
    assertThat(service.saveProcess(createProcess(pipelineName, ProcessState.FAILED, 1)).getState())
        .isEqualTo(ProcessState.FAILED);
    assertThat(service.saveProcess(createProcess(pipelineName, ProcessState.PENDING, 1)).getState())
        .isEqualTo(ProcessState.PENDING);
    assertThat(service.saveProcess(createProcess(pipelineName, ProcessState.PENDING, 1)).getState())
        .isEqualTo(ProcessState.PENDING);

    assertThat(service.getAvailableActiveProcesses(pipelineName, DEFAULT_LIMIT)).hasSize(2);
    assertThat(service.getCompletedProcesses(pipelineName, DEFAULT_LIMIT)).hasSize(3);
    assertThat(service.getFailedProcesses(pipelineName, DEFAULT_LIMIT)).hasSize(4);
    assertThat(service.getPendingProcesses(pipelineName, DEFAULT_LIMIT)).hasSize(2);
  }

  public void testGetActiveCompletedFailedPendingDifferentPriority() {
    String pipelineName = UniqueStringGenerator.randomPipelineName();
    String launcherName = UniqueStringGenerator.randomLauncherName();

    assertThat(service.saveProcess(createProcess(pipelineName, ProcessState.ACTIVE, 1)).getState())
        .isEqualTo(ProcessState.ACTIVE);
    assertThat(service.saveProcess(createProcess(pipelineName, ProcessState.ACTIVE, 2)).getState())
        .isEqualTo(ProcessState.ACTIVE);
    assertThat(
            service.saveProcess(createProcess(pipelineName, ProcessState.COMPLETED, 1)).getState())
        .isEqualTo(ProcessState.COMPLETED);
    assertThat(
            service.saveProcess(createProcess(pipelineName, ProcessState.COMPLETED, 2)).getState())
        .isEqualTo(ProcessState.COMPLETED);
    assertThat(
            service.saveProcess(createProcess(pipelineName, ProcessState.COMPLETED, 3)).getState())
        .isEqualTo(ProcessState.COMPLETED);
    assertThat(service.saveProcess(createProcess(pipelineName, ProcessState.FAILED, 1)).getState())
        .isEqualTo(ProcessState.FAILED);
    assertThat(service.saveProcess(createProcess(pipelineName, ProcessState.FAILED, 2)).getState())
        .isEqualTo(ProcessState.FAILED);
    assertThat(service.saveProcess(createProcess(pipelineName, ProcessState.FAILED, 3)).getState())
        .isEqualTo(ProcessState.FAILED);
    assertThat(service.saveProcess(createProcess(pipelineName, ProcessState.FAILED, 4)).getState())
        .isEqualTo(ProcessState.FAILED);
    assertThat(service.saveProcess(createProcess(pipelineName, ProcessState.PENDING, 1)).getState())
        .isEqualTo(ProcessState.PENDING);
    assertThat(service.saveProcess(createProcess(pipelineName, ProcessState.PENDING, 2)).getState())
        .isEqualTo(ProcessState.PENDING);

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

  public void testGetProcess() {
    String pipelineName1 = UniqueStringGenerator.randomPipelineName();
    String pipelineName2 = UniqueStringGenerator.randomPipelineName();

    ProcessEntity processActive1 =
        service.saveProcess(createProcess(pipelineName1, ProcessState.ACTIVE, 1));
    ProcessEntity processActive2 =
        service.saveProcess(createProcess(pipelineName1, ProcessState.ACTIVE, 1));
    ProcessEntity processCompleted1 =
        service.saveProcess(createProcess(pipelineName1, ProcessState.COMPLETED, 1));
    ProcessEntity processCompleted2 =
        service.saveProcess(createProcess(pipelineName1, ProcessState.COMPLETED, 1));
    ProcessEntity processFailed1 =
        service.saveProcess(createProcess(pipelineName1, ProcessState.FAILED, 1));
    ProcessEntity processFailed2 =
        service.saveProcess(createProcess(pipelineName1, ProcessState.FAILED, 1));
    ProcessEntity processPending1 =
        service.saveProcess(createProcess(pipelineName1, ProcessState.PENDING, 1));
    ProcessEntity processPending2 =
        service.saveProcess(createProcess(pipelineName1, ProcessState.PENDING, 1));

    ProcessEntity processActive3 =
        service.saveProcess(createProcess(pipelineName2, ProcessState.ACTIVE, 1));
    ProcessEntity processActive4 =
        service.saveProcess(createProcess(pipelineName2, ProcessState.ACTIVE, 1));
    ProcessEntity processCompleted3 =
        service.saveProcess(createProcess(pipelineName2, ProcessState.COMPLETED, 1));
    ProcessEntity processCompleted4 =
        service.saveProcess(createProcess(pipelineName2, ProcessState.COMPLETED, 1));
    ProcessEntity processFailed3 =
        service.saveProcess(createProcess(pipelineName2, ProcessState.FAILED, 1));
    ProcessEntity processFailed4 =
        service.saveProcess(createProcess(pipelineName2, ProcessState.FAILED, 1));
    ProcessEntity processPending3 =
        service.saveProcess(createProcess(pipelineName2, ProcessState.PENDING, 1));
    ProcessEntity processPending4 =
        service.saveProcess(createProcess(pipelineName2, ProcessState.PENDING, 1));

    assertThat(processActive1.getState()).isEqualTo(ProcessState.ACTIVE);
    assertThat(processActive2.getState()).isEqualTo(ProcessState.ACTIVE);
    assertThat(processCompleted1.getState()).isEqualTo(ProcessState.COMPLETED);
    assertThat(processCompleted2.getState()).isEqualTo(ProcessState.COMPLETED);
    assertThat(processFailed1.getState()).isEqualTo(ProcessState.FAILED);
    assertThat(processFailed2.getState()).isEqualTo(ProcessState.FAILED);
    assertThat(processPending1.getState()).isEqualTo(ProcessState.PENDING);
    assertThat(processPending2.getState()).isEqualTo(ProcessState.PENDING);

    assertThat(processActive3.getState()).isEqualTo(ProcessState.ACTIVE);
    assertThat(processActive4.getState()).isEqualTo(ProcessState.ACTIVE);
    assertThat(processCompleted3.getState()).isEqualTo(ProcessState.COMPLETED);
    assertThat(processCompleted4.getState()).isEqualTo(ProcessState.COMPLETED);
    assertThat(processFailed3.getState()).isEqualTo(ProcessState.FAILED);
    assertThat(processFailed4.getState()).isEqualTo(ProcessState.FAILED);
    assertThat(processPending3.getState()).isEqualTo(ProcessState.PENDING);
    assertThat(processPending4.getState()).isEqualTo(ProcessState.PENDING);

    // Test pipeline name.
    testProcessStateCount(
        service.getProcesses(pipelineName1, null /* processId */, null /* state*/, DEFAULT_LIMIT),
        2,
        2,
        2,
        2);

    // Test pipeline name and process id.
    for (ProcessEntity process :
        Arrays.asList(processActive1, processCompleted1, processFailed1, processPending1)) {
      testProcessStateCount(
          service.getProcesses(
              pipelineName1, process.getProcessId(), null /* state*/, DEFAULT_LIMIT),
          process.getProcessId().equals(processCompleted1.getProcessId()) ? 1 : 0,
          process.getProcessId().equals(processActive1.getProcessId()) ? 1 : 0,
          process.getProcessId().equals(processFailed1.getProcessId()) ? 1 : 0,
          process.getProcessId().equals(processPending1.getProcessId()) ? 1 : 0);
    }

    // Test pipeline name and state.
    for (ProcessState state :
        EnumSet.of(
            ProcessState.ACTIVE,
            ProcessState.COMPLETED,
            ProcessState.FAILED,
            ProcessState.CANCELLED)) {
      testProcessStateCount(
          service.getProcesses(pipelineName1, null /* processId */, state, DEFAULT_LIMIT),
          state == ProcessState.COMPLETED ? 2 : 0,
          state == ProcessState.ACTIVE ? 2 : 0,
          state == ProcessState.FAILED ? 2 : 0,
          state == ProcessState.PENDING ? 2 : 0);
    }

    // Test pipeline name, process id and state.
    for (ProcessEntity process :
        Arrays.asList(processActive1, processCompleted1, processFailed1, processPending1)) {
      testProcessStateCount(
          service.getProcesses(
              pipelineName1, process.getProcessId(), process.getState(), DEFAULT_LIMIT),
          process.getProcessId().equals(processCompleted1.getProcessId()) ? 1 : 0,
          process.getProcessId().equals(processActive1.getProcessId()) ? 1 : 0,
          process.getProcessId().equals(processFailed1.getProcessId()) ? 1 : 0,
          process.getProcessId().equals(processPending1.getProcessId()) ? 1 : 0);
    }

    // Test process id.
    for (ProcessEntity process :
        Arrays.asList(processActive1, processCompleted1, processFailed1, processPending1)) {
      testProcessStateCount(
          service.getProcesses(null, process.getProcessId(), null /* state*/, DEFAULT_LIMIT),
          process.getProcessId().equals(processCompleted1.getProcessId()) ? 1 : 0,
          process.getProcessId().equals(processActive1.getProcessId()) ? 1 : 0,
          process.getProcessId().equals(processFailed1.getProcessId()) ? 1 : 0,
          process.getProcessId().equals(processPending1.getProcessId()) ? 1 : 0);
    }
    for (ProcessEntity process :
        Arrays.asList(processActive3, processCompleted3, processFailed3, processPending3)) {
      testProcessStateCount(
          service.getProcesses(null, process.getProcessId(), null /* state*/, DEFAULT_LIMIT),
          process.getProcessId().equals(processCompleted3.getProcessId()) ? 1 : 0,
          process.getProcessId().equals(processActive3.getProcessId()) ? 1 : 0,
          process.getProcessId().equals(processFailed3.getProcessId()) ? 1 : 0,
          process.getProcessId().equals(processPending3.getProcessId()) ? 1 : 0);
    }

    // Test process id and state.
    for (ProcessEntity process :
        Arrays.asList(processActive1, processCompleted1, processFailed1, processPending1)) {
      testProcessStateCount(
          service.getProcesses(null, process.getProcessId(), process.getState(), DEFAULT_LIMIT),
          process.getProcessId().equals(processCompleted1.getProcessId()) ? 1 : 0,
          process.getProcessId().equals(processActive1.getProcessId()) ? 1 : 0,
          process.getProcessId().equals(processFailed1.getProcessId()) ? 1 : 0,
          process.getProcessId().equals(processPending1.getProcessId()) ? 1 : 0);
    }

    // Test state.
    /*
    for (ProcessState state :
            EnumSet.of(
                    ProcessState.ACTIVE,
                    ProcessState.COMPLETED,
                    ProcessState.FAILED,
                    ProcessState.CANCELLED)) {
      testProcessStateCount(
              service.getProcesses(null, null, state, DEFAULT_LIMIT),
              state == ProcessState.COMPLETED ? 4 : 0,
              state == ProcessState.ACTIVE ? 4 : 0,
              state == ProcessState.FAILED ? 4 : 0,
              state == ProcessState.PENDING ? 4 : 0);
    }
    */

    // Test none.
    /*
    for (ProcessState state :
            EnumSet.of(
                    ProcessState.ACTIVE,
                    ProcessState.COMPLETED,
                    ProcessState.FAILED,
                    ProcessState.CANCELLED)) {
      testProcessStateCount(
              service.getProcesses(null, null, null, DEFAULT_LIMIT),
              4,
              4,
              4,
              4);
    }
    */
  }

  private static ProcessEntity createProcess(
      String pipelineName, ProcessState state, int priority) {
    ProcessEntity process =
        ProcessEntity.createExecution(
            pipelineName, UniqueStringGenerator.randomProcessId(), priority);
    process.setState(state);
    process.setExecutionCount(0);
    return process;
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
