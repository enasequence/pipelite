package pipelite.service;

import org.junit.jupiter.api.Test;
import pipelite.UniqueStringGenerator;
import pipelite.entity.PipeliteStage;
import pipelite.executor.SuccessTaskExecutor;
import pipelite.task.TaskExecutionResult;
import pipelite.task.TaskInstance;

import static org.assertj.core.api.Assertions.assertThat;

class PipeliteInMemoryStageServiceTest {

  @Test
  public void testCrud() {

    PipeliteInMemoryStageService service = new PipeliteInMemoryStageService();

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
