package pipelite.service;

import org.junit.jupiter.api.Test;
import pipelite.RandomStringGenerator;
import pipelite.entity.PipeliteStage;
import pipelite.task.result.TaskExecutionResult;

import static org.assertj.core.api.Assertions.assertThat;

class PipeliteInMemoryStageServiceTest {

  @Test
  public void testCrud() {

    PipeliteInMemoryStageService service = new PipeliteInMemoryStageService();

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
