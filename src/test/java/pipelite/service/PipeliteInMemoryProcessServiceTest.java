package pipelite.service;

import org.junit.jupiter.api.Test;
import pipelite.UniqueStringGenerator;
import pipelite.entity.PipeliteProcess;
import pipelite.process.ProcessExecutionState;

import java.util.Comparator;

import static org.assertj.core.api.Assertions.assertThat;

class PipeliteInMemoryProcessServiceTest {

  private static final String PROCESS_NAME = UniqueStringGenerator.randomProcessName();

  @Test
  public void testCrud() {

    PipeliteInMemoryProcessService service = new PipeliteInMemoryProcessService();

    String processId = UniqueStringGenerator.randomProcessId();
    ProcessExecutionState state = ProcessExecutionState.ACTIVE;
    Integer execCnt = 3;
    Integer priority = null;

    PipeliteProcess process =
        new PipeliteProcess(processId, PROCESS_NAME, state, execCnt, priority);

    service.saveProcess(process);

    assertThat(service.getSavedProcess(PROCESS_NAME, processId).get()).isEqualTo(process);

    process.setState(ProcessExecutionState.COMPLETED);
    process.setExecutionCount(4);
    process.setPriority(9);

    service.saveProcess(process);

    assertThat(service.getSavedProcess(PROCESS_NAME, processId).get()).isEqualTo(process);

    service.delete(process);

    assertThat(service.getSavedProcess(PROCESS_NAME, processId).isPresent()).isFalse();
  }

  @Test
  public void testReportsSamePriority() {

    PipeliteInMemoryProcessService service = new PipeliteInMemoryProcessService();

    service.saveProcess(createPipeliteProcess(ProcessExecutionState.ACTIVE, 1));
    service.saveProcess(createPipeliteProcess(ProcessExecutionState.ACTIVE, 1));
    service.saveProcess(createPipeliteProcess(ProcessExecutionState.COMPLETED, 1));
    service.saveProcess(createPipeliteProcess(ProcessExecutionState.COMPLETED, 1));
    service.saveProcess(createPipeliteProcess(ProcessExecutionState.COMPLETED, 1));
    service.saveProcess(createPipeliteProcess(ProcessExecutionState.FAILED, 1));
    service.saveProcess(createPipeliteProcess(ProcessExecutionState.FAILED, 1));
    service.saveProcess(createPipeliteProcess(ProcessExecutionState.FAILED, 1));
    service.saveProcess(createPipeliteProcess(ProcessExecutionState.FAILED, 1));

    assertThat(service.getActiveProcesses(PROCESS_NAME)).hasSize(2);
    assertThat(service.getCompletedProcesses(PROCESS_NAME)).hasSize(3);
    assertThat(service.getFailedProcesses(PROCESS_NAME)).hasSize(4);
  }

  @Test
  public void testReportsDifferentPriority() {

    PipeliteInMemoryProcessService service = new PipeliteInMemoryProcessService();

    service.saveProcess(createPipeliteProcess(ProcessExecutionState.ACTIVE, 1));
    service.saveProcess(createPipeliteProcess(ProcessExecutionState.ACTIVE, 2));
    service.saveProcess(createPipeliteProcess(ProcessExecutionState.COMPLETED, 1));
    service.saveProcess(createPipeliteProcess(ProcessExecutionState.COMPLETED, 2));
    service.saveProcess(createPipeliteProcess(ProcessExecutionState.COMPLETED, 3));
    service.saveProcess(createPipeliteProcess(ProcessExecutionState.FAILED, 1));
    service.saveProcess(createPipeliteProcess(ProcessExecutionState.FAILED, 2));
    service.saveProcess(createPipeliteProcess(ProcessExecutionState.FAILED, 4));
    service.saveProcess(createPipeliteProcess(ProcessExecutionState.FAILED, 3));

    assertThat(service.getActiveProcesses(PROCESS_NAME)).hasSize(2);
    assertThat(service.getCompletedProcesses(PROCESS_NAME)).hasSize(3);
    assertThat(service.getFailedProcesses(PROCESS_NAME)).hasSize(4);

    assertThat(service.getActiveProcesses(PROCESS_NAME))
            .isSortedAccordingTo(Comparator.comparingInt(PipeliteProcess::getPriority).reversed());
    assertThat(service.getFailedProcesses(PROCESS_NAME))
            .isSortedAccordingTo(Comparator.comparingInt(PipeliteProcess::getPriority).reversed());
  }

  private static PipeliteProcess createPipeliteProcess(ProcessExecutionState state, int priority) {
    return new PipeliteProcess(
        UniqueStringGenerator.randomProcessId(), PROCESS_NAME, state, 0, priority);
  }
}
