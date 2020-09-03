package pipelite.executor.call;

import org.junit.jupiter.api.Test;
import pipelite.UniqueStringGenerator;
import pipelite.task.TaskInstance;
import pipelite.task.TaskParameters;
import pipelite.task.TaskExecutionResult;
import pipelite.task.TaskExecutionResultType;

import static org.assertj.core.api.Assertions.assertThat;

public class SystemCallExecutorTest {

  @Test
  public void test() {

    SystemCallExecutor executor =
        SystemCallExecutor.builder().cmd(taskInstance -> "echo test").build();

    String processName = UniqueStringGenerator.randomProcessName();
    String processId = UniqueStringGenerator.randomProcessId();
    String taskName = UniqueStringGenerator.randomTaskName();

    TaskParameters taskParameters = TaskParameters.builder().build();

    TaskInstance taskInstance =
        TaskInstance.builder()
            .processName(processName)
            .processId(processId)
            .taskName(taskName)
            // Executor is not required by SystemCallTaskExecutor
            // .executor()
            .taskParameters(taskParameters)
            .build();

    TaskExecutionResult result = executor.execute(taskInstance);
    assertThat(result.getResultType()).isEqualTo(TaskExecutionResultType.SUCCESS);
    assertThat(result.getAttribute(TaskExecutionResult.COMMAND))
        .isEqualTo("echo test");
    assertThat(result.getAttribute(TaskExecutionResult.EXIT_CODE))
        .isEqualTo("0");
    assertThat(result.getStdout())
        .isEqualTo("test\n");
  }
}
