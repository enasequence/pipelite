package pipelite.repository;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.Rollback;
import org.springframework.test.context.ActiveProfiles;
import pipelite.RandomStringGenerator;
import pipelite.TestConfiguration;
import pipelite.entity.PipeliteProcess;
import pipelite.entity.PipeliteProcessId;
import pipelite.process.state.ProcessExecutionState;

import javax.transaction.Transactional;

import java.util.Comparator;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(classes = TestConfiguration.class)
@ActiveProfiles("test")
class PipeliteProcessRepositoryTest {

  @Autowired PipeliteProcessRepository repository;

  private static final String PROCESS_NAME = RandomStringGenerator.randomProcessName();

  @Test
  @Transactional
  @Rollback
  public void testCrud() {

    String processId = RandomStringGenerator.randomProcessId();
    ProcessExecutionState state = ProcessExecutionState.ACTIVE;
    Integer execCnt = 3;
    Integer priority = null;
    PipeliteProcessId id = new PipeliteProcessId(processId, PROCESS_NAME);

    PipeliteProcess process =
        new PipeliteProcess(processId, PROCESS_NAME, state, execCnt, priority);

    repository.save(process);

    assertThat(repository.findById(id).get()).isEqualTo(process);

    process.setState(ProcessExecutionState.COMPLETED);
    process.setExecutionCount(4);
    process.setPriority(9);

    repository.save(process);

    assertThat(repository.findById(id).get()).isEqualTo(process);

    repository.delete(process);

    assertThat(repository.findById(id).isPresent()).isFalse();
  }

  @Test
  @Transactional
  @Rollback
  public void findAllByProcessNameAndState() {
    repository.save(createPipeliteProcess(ProcessExecutionState.ACTIVE, 1));
    repository.save(createPipeliteProcess(ProcessExecutionState.ACTIVE, 1));
    repository.save(createPipeliteProcess(ProcessExecutionState.COMPLETED, 1));
    repository.save(createPipeliteProcess(ProcessExecutionState.COMPLETED, 1));
    repository.save(createPipeliteProcess(ProcessExecutionState.COMPLETED, 1));
    repository.save(createPipeliteProcess(ProcessExecutionState.FAILED, 1));
    repository.save(createPipeliteProcess(ProcessExecutionState.FAILED, 1));
    repository.save(createPipeliteProcess(ProcessExecutionState.FAILED, 1));
    repository.save(createPipeliteProcess(ProcessExecutionState.FAILED, 1));

    assertThat(repository.findAllByProcessNameAndState(PROCESS_NAME, ProcessExecutionState.ACTIVE))
        .hasSize(2);
    assertThat(
            repository.findAllByProcessNameAndState(PROCESS_NAME, ProcessExecutionState.COMPLETED))
        .hasSize(3);
    assertThat(repository.findAllByProcessNameAndState(PROCESS_NAME, ProcessExecutionState.FAILED))
        .hasSize(4);
  }

  @Test
  @Transactional
  @Rollback
  public void findAllByProcessNameAndStateOrderByPriorityDesc() {
    repository.save(createPipeliteProcess(ProcessExecutionState.ACTIVE, 1));
    repository.save(createPipeliteProcess(ProcessExecutionState.ACTIVE, 2));
    repository.save(createPipeliteProcess(ProcessExecutionState.COMPLETED, 1));
    repository.save(createPipeliteProcess(ProcessExecutionState.COMPLETED, 2));
    repository.save(createPipeliteProcess(ProcessExecutionState.COMPLETED, 3));
    repository.save(createPipeliteProcess(ProcessExecutionState.FAILED, 1));
    repository.save(createPipeliteProcess(ProcessExecutionState.FAILED, 2));
    repository.save(createPipeliteProcess(ProcessExecutionState.FAILED, 4));
    repository.save(createPipeliteProcess(ProcessExecutionState.FAILED, 3));

    assertThat(
            repository.findAllByProcessNameAndStateOrderByPriorityDesc(
                PROCESS_NAME, ProcessExecutionState.ACTIVE))
        .hasSize(2);
    assertThat(
            repository.findAllByProcessNameAndStateOrderByPriorityDesc(
                PROCESS_NAME, ProcessExecutionState.COMPLETED))
        .hasSize(3);
    assertThat(
            repository.findAllByProcessNameAndStateOrderByPriorityDesc(
                PROCESS_NAME, ProcessExecutionState.FAILED))
        .hasSize(4);

    assertThat(
            repository.findAllByProcessNameAndStateOrderByPriorityDesc(
                PROCESS_NAME, ProcessExecutionState.ACTIVE))
        .isSortedAccordingTo(Comparator.comparingInt(PipeliteProcess::getPriority).reversed());
    assertThat(
            repository.findAllByProcessNameAndStateOrderByPriorityDesc(
                PROCESS_NAME, ProcessExecutionState.COMPLETED))
        .isSortedAccordingTo(Comparator.comparingInt(PipeliteProcess::getPriority).reversed());
    assertThat(
            repository.findAllByProcessNameAndStateOrderByPriorityDesc(
                PROCESS_NAME, ProcessExecutionState.FAILED))
        .isSortedAccordingTo(Comparator.comparingInt(PipeliteProcess::getPriority).reversed());
  }

  private static PipeliteProcess createPipeliteProcess(ProcessExecutionState state, int priority) {
    return new PipeliteProcess(
        RandomStringGenerator.randomProcessId(), PROCESS_NAME, state, 0, priority);
  }
}
