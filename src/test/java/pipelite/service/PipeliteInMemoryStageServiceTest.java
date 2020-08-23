package pipelite.service;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.Rollback;
import org.springframework.test.context.ActiveProfiles;
import pipelite.RandomStringGenerator;
import pipelite.TestConfiguration;
import pipelite.entity.PipeliteStage;
import pipelite.task.result.TaskExecutionResult;

import javax.transaction.Transactional;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(classes = TestConfiguration.class)
@ActiveProfiles("test")
class PipeliteInMemoryStageServiceTest {

  @Autowired PipeliteInMemoryStageService service;

  @Test
  public void testCrud() {

    String processId = RandomStringGenerator.randomProcessId();
    String processName = RandomStringGenerator.randomProcessName();
    String stageName = RandomStringGenerator.randomStageName();

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
