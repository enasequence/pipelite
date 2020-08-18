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
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(classes = TestConfiguration.class)
@ActiveProfiles("test")
class PipeliteProcessRepositoryTest {

  @Autowired PipeliteProcessRepository repository;

  @Test
  @Transactional
  @Rollback
  public void test() {

    String processId = RandomStringGenerator.randomProcessId();
    String processName = RandomStringGenerator.randomProcessName();
    ProcessExecutionState state = ProcessExecutionState.ACTIVE;
    Integer execCnt = 3;
    Integer priority = null;
    PipeliteProcessId id = new PipeliteProcessId(processId, processName);

    PipeliteProcess process = new PipeliteProcess(processId, processName, state, execCnt, priority);

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
}
