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

    PipeliteProcess entity = new PipeliteProcess(processId, processName, state, execCnt, priority);

    repository.save(entity);

    Optional<PipeliteProcess> savedEntity =
        repository.findById(new PipeliteProcessId(processId, processName));
    assertThat(savedEntity.isPresent()).isTrue();
    assertThat(savedEntity.get().getProcessId()).isEqualTo(entity.getProcessId());
    assertThat(savedEntity.get().getProcessName()).isEqualTo(entity.getProcessName());
    assertThat(savedEntity.get().getState()).isEqualTo(entity.getState());
    assertThat(savedEntity.get().getExecutionCount()).isEqualTo(entity.getExecutionCount());
    assertThat(savedEntity.get().getPriority()).isNull();

    entity.setState(ProcessExecutionState.COMPLETED);
    entity.setExecutionCount(4);
    entity.setPriority(9);

    repository.save(entity);

    savedEntity = repository.findById(new PipeliteProcessId(processId, processName));
    assertThat(savedEntity.isPresent()).isTrue();
    assertThat(savedEntity.get().getProcessId()).isEqualTo(entity.getProcessId());
    assertThat(savedEntity.get().getProcessName()).isEqualTo(entity.getProcessName());
    assertThat(savedEntity.get().getState()).isEqualTo(entity.getState());
    assertThat(savedEntity.get().getExecutionCount()).isEqualTo(entity.getExecutionCount());
    assertThat(savedEntity.get().getPriority()).isEqualTo(9);

    repository.delete(entity);

    savedEntity = repository.findById(new PipeliteProcessId(processId, processName));
    assertThat(savedEntity.isPresent()).isFalse();
  }
}
