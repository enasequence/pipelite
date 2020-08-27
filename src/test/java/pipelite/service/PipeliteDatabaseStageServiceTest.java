package pipelite.service;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.Rollback;
import org.springframework.test.context.ActiveProfiles;
import pipelite.UniqueStringGenerator;
import pipelite.FullTestConfiguration;
import pipelite.entity.PipeliteStage;
import pipelite.task.TaskExecutionResult;

import org.springframework.transaction.annotation.Transactional;

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
    String stageName = UniqueStringGenerator.randomTaskName();

    PipeliteStage stage = PipeliteStage.newExecution(processId, processName, stageName);

    service.saveStage(stage);

    assertThat(service.getSavedStage(processName, processId, stageName).get()).isEqualTo(stage);

    stage.endExecution(TaskExecutionResult.success(), "executionCmd", "stdOut", "stdErr");

    service.saveStage(stage);

    assertThat(service.getSavedStage(processName, processId, stageName).get()).isEqualTo(stage);

    service.delete(stage);

    assertThat(service.getSavedStage(processName, processId, stageName).isPresent()).isFalse();
  }
}
