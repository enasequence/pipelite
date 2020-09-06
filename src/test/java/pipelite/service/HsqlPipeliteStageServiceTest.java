package pipelite.service;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.Rollback;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.transaction.annotation.Transactional;
import pipelite.FullTestConfiguration;
import pipelite.UniqueStringGenerator;
import pipelite.entity.PipeliteStage;
import pipelite.task.TaskExecutionResult;
import pipelite.task.TaskInstance;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(classes = FullTestConfiguration.class)
@ActiveProfiles(value = {"hsql-test"})
class HsqlPipeliteStageServiceTest {

  @Autowired
  PipeliteStageService service;

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
