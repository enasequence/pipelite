package pipelite.service;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.Rollback;
import org.springframework.test.context.ActiveProfiles;
import pipelite.RandomStringGenerator;
import pipelite.FullTestConfiguration;
import pipelite.entity.PipeliteProcess;
import pipelite.process.ProcessExecutionState;

import org.springframework.transaction.annotation.Transactional;

import java.util.Comparator;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(classes = FullTestConfiguration.class)
@ActiveProfiles(value = {"database", "database-oracle-test"})
class PipeliteDatabaseProcessServiceTest {

  @Autowired PipeliteDatabaseProcessService service;

  private static final String PROCESS_NAME = RandomStringGenerator.randomProcessName();

  @Test
  @Transactional
  @Rollback
  public void testCrud() {

    String processId = RandomStringGenerator.randomProcessId();
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
  @Transactional
  @Rollback
  public void testReportsSamePriority() {
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
  @Transactional
  @Rollback
  public void testReportsDifferentPriority() {
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
        RandomStringGenerator.randomProcessId(), PROCESS_NAME, state, 0, priority);
  }
}
