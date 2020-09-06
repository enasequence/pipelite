package pipelite.service;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.Rollback;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.transaction.annotation.Transactional;
import pipelite.FullTestConfiguration;
import pipelite.UniqueStringGenerator;
import pipelite.entity.PipeliteProcess;
import pipelite.process.ProcessExecutionState;

import java.util.Comparator;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(classes = FullTestConfiguration.class)
@ActiveProfiles(value = {"hsql-test"})
class HsqlPipeliteProcessServiceTest {

  @Autowired
  PipeliteProcessService service;

  @Test
  @Transactional
  @Rollback
  public void testCrud() {

    String processName = UniqueStringGenerator.randomProcessName();
    String processId = UniqueStringGenerator.randomProcessId();
    ProcessExecutionState state = ProcessExecutionState.ACTIVE;
    Integer execCnt = 3;
    Integer priority = 0;

    PipeliteProcess process = new PipeliteProcess(processId, processName, state, execCnt, priority);

    service.saveProcess(process);

    assertThat(service.getSavedProcess(processName, processId).get()).isEqualTo(process);

    process.setState(ProcessExecutionState.COMPLETED);
    process.setExecutionCount(4);
    process.setPriority(9);

    service.saveProcess(process);

    assertThat(service.getSavedProcess(processName, processId).get()).isEqualTo(process);

    service.delete(process);

    assertThat(service.getSavedProcess(processName, processId).isPresent()).isFalse();
  }

  @Test
  @Transactional
  @Rollback
  public void testReportsSamePriority() {
    String processName = UniqueStringGenerator.randomProcessName();

    service.saveProcess(createPipeliteProcess(processName, ProcessExecutionState.ACTIVE, 1));
    service.saveProcess(createPipeliteProcess(processName, ProcessExecutionState.ACTIVE, 1));
    service.saveProcess(createPipeliteProcess(processName, ProcessExecutionState.COMPLETED, 1));
    service.saveProcess(createPipeliteProcess(processName, ProcessExecutionState.COMPLETED, 1));
    service.saveProcess(createPipeliteProcess(processName, ProcessExecutionState.COMPLETED, 1));
    service.saveProcess(createPipeliteProcess(processName, ProcessExecutionState.FAILED, 1));
    service.saveProcess(createPipeliteProcess(processName, ProcessExecutionState.FAILED, 1));
    service.saveProcess(createPipeliteProcess(processName, ProcessExecutionState.FAILED, 1));
    service.saveProcess(createPipeliteProcess(processName, ProcessExecutionState.FAILED, 1));

    assertThat(service.getActiveProcesses(processName)).hasSize(2);
    assertThat(service.getCompletedProcesses(processName)).hasSize(3);
    assertThat(service.getFailedProcesses(processName)).hasSize(4);
  }

  @Test
  @Transactional
  @Rollback
  public void testReportsDifferentPriority() {
    String processName = UniqueStringGenerator.randomProcessName();

    service.saveProcess(createPipeliteProcess(processName, ProcessExecutionState.ACTIVE, 1));
    service.saveProcess(createPipeliteProcess(processName, ProcessExecutionState.ACTIVE, 2));
    service.saveProcess(createPipeliteProcess(processName, ProcessExecutionState.COMPLETED, 1));
    service.saveProcess(createPipeliteProcess(processName, ProcessExecutionState.COMPLETED, 2));
    service.saveProcess(createPipeliteProcess(processName, ProcessExecutionState.COMPLETED, 3));
    service.saveProcess(createPipeliteProcess(processName, ProcessExecutionState.FAILED, 1));
    service.saveProcess(createPipeliteProcess(processName, ProcessExecutionState.FAILED, 2));
    service.saveProcess(createPipeliteProcess(processName, ProcessExecutionState.FAILED, 4));
    service.saveProcess(createPipeliteProcess(processName, ProcessExecutionState.FAILED, 3));

    assertThat(service.getActiveProcesses(processName)).hasSize(2);
    assertThat(service.getCompletedProcesses(processName)).hasSize(3);
    assertThat(service.getFailedProcesses(processName)).hasSize(4);

    assertThat(service.getActiveProcesses(processName))
        .isSortedAccordingTo(Comparator.comparingInt(PipeliteProcess::getPriority).reversed());
    assertThat(service.getFailedProcesses(processName))
        .isSortedAccordingTo(Comparator.comparingInt(PipeliteProcess::getPriority).reversed());
  }

  private static PipeliteProcess createPipeliteProcess(
      String processName, ProcessExecutionState state, int priority) {
    return new PipeliteProcess(
        UniqueStringGenerator.randomProcessId(), processName, state, 0, priority);
  }
}
