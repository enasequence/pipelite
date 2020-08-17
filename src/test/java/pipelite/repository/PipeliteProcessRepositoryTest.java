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
import pipelite.task.state.TaskExecutionState;

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
    TaskExecutionState state = TaskExecutionState.COMPLETED;
    Integer execCnt = 3;
    Integer priority = null;

    PipeliteProcess e1 = new PipeliteProcess(processId, processName, state, execCnt, priority);

    repository.save(e1);

    Optional<PipeliteProcess> e2 =
        repository.findById(new PipeliteProcessId(processId, processName));

    assertThat(e2.isPresent()).isTrue();
    assertThat(e2.get().getProcessId()).isEqualTo(processId);
    assertThat(e2.get().getProcessName()).isEqualTo(processName);
    assertThat(e2.get().getState()).isEqualTo(state);
    assertThat(e2.get().getExecCnt()).isEqualTo(3);
    assertThat(e2.get().getPriority()).isNull();
  }
}
