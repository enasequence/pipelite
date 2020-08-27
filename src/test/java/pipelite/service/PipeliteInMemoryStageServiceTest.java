package pipelite.service;

import org.junit.jupiter.api.Test;
import pipelite.UniqueStringGenerator;
import pipelite.entity.PipeliteStage;
import pipelite.task.TaskExecutionResult;

import static org.assertj.core.api.Assertions.assertThat;

class PipeliteInMemoryStageServiceTest {

  @Test
  public void testCrud() {

    PipeliteInMemoryStageService service = new PipeliteInMemoryStageService();

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
