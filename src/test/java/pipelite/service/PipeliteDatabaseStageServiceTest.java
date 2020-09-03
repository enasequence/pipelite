package pipelite.service;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.Rollback;
import org.springframework.test.context.ActiveProfiles;
import pipelite.UniqueStringGenerator;
import pipelite.FullTestConfiguration;
import pipelite.entity.PipeliteStage;
import pipelite.executor.SuccessTaskExecutor;
import pipelite.task.TaskExecutionResult;

import org.springframework.transaction.annotation.Transactional;
import pipelite.task.TaskInstance;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(classes = FullTestConfiguration.class)
@ActiveProfiles(value = {"database", "database-oracle-test"})
class PipeliteDatabaseStageServiceTest {

  @Autowired PipeliteDatabaseStageService service;

  @Test
  @Transactional
  @Rollback
  public void testCrud() {

    String processId = UniqueStringGenerator.randomProcessId();
    String processName = UniqueStringGenerator.randomProcessName();
    String taskName = UniqueStringGenerator.randomTaskName();

    TaskInstance taskInstance =
        TaskInstance.builder()
            .processName(processName)
            .processId(processId)
            .taskName(taskName)
            .build();

    PipeliteStage stage = PipeliteStage.createExecution(taskInstance);

    service.saveStage(stage);

    assertThat(service.getSavedStage(processName, processId, taskName).get()).isEqualTo(stage);

    stage.endExecution(TaskExecutionResult.success());

    service.saveStage(stage);

    assertThat(service.getSavedStage(processName, processId, taskName).get()).isEqualTo(stage);

    service.delete(stage);

    assertThat(service.getSavedStage(processName, processId, taskName).isPresent()).isFalse();
  }
}
