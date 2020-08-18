package pipelite.repository;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.Rollback;
import org.springframework.test.context.ActiveProfiles;
import pipelite.RandomStringGenerator;
import pipelite.TestConfiguration;
import pipelite.entity.PipeliteStage;
import pipelite.entity.PipeliteStageId;
import pipelite.task.result.TaskExecutionResult;

import javax.transaction.Transactional;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(classes = TestConfiguration.class)
@ActiveProfiles("test")
class PipeliteStageRepositoryTest {

  @Autowired PipeliteStageRepository repository;

  @Test
  @Transactional
  @Rollback
  public void test() {

    String processId = RandomStringGenerator.randomProcessId();
    String processName = RandomStringGenerator.randomProcessName();
    String stageName = RandomStringGenerator.randomStageName();
    String executionId = RandomStringGenerator.randomExecutionId();

    PipeliteStageId id = new PipeliteStageId(processId, processName, stageName);

    PipeliteStage stage =
        PipeliteStage.newExecution(processId, processName, stageName, executionId);

    repository.save(stage);

    assertThat(repository.findById(id).get()).isEqualTo(stage);

    executionId = RandomStringGenerator.randomExecutionId();

    stage.retryExecution(executionId);

    repository.save(stage);

    assertThat(repository.findById(id).get()).isEqualTo(stage);

    stage.endExecution(TaskExecutionResult.success(), "executionCmd", "stdOut", "stdErr");

    repository.save(stage);

    assertThat(repository.findById(id).get()).isEqualTo(stage);

    repository.delete(stage);

    assertThat(repository.findById(id).isPresent()).isFalse();
  }
}
