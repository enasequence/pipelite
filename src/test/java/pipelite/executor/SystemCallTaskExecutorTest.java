package pipelite.executor;

import org.junit.jupiter.api.Test;
import pipelite.UniqueStringGenerator;
import pipelite.instance.TaskInstance;
import pipelite.instance.TaskParameters;
import pipelite.resolver.ResultResolver;
import pipelite.task.TaskExecutionResult;
import pipelite.task.TaskExecutionResultType;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

public class SystemCallTaskExecutorTest {

  @Test
  public void test() {

    SystemCallTaskExecutor taskExecutor =
        SystemCallTaskExecutor.builder()
            .executable("echo")
            .arguments(Arrays.asList("test"))
            .build();

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
            .resolver(ResultResolver.DEFAULT_EXIT_CODE_RESOLVER)
            .taskParameters(taskParameters)
            .build();

    TaskExecutionResult result = taskExecutor.execute(taskInstance);
    assertThat(result.getResultType()).isEqualTo(TaskExecutionResultType.SUCCESS);
    assertThat(result.getAttribute(TaskExecutionResult.STANDARD_ATTRIBUTE_COMMAND))
        .isEqualTo("echo test");
    assertThat(result.getAttribute(TaskExecutionResult.STANDARD_ATTRIBUTE_EXIT_CODE))
        .isEqualTo("0");
    assertThat(result.getAttribute(TaskExecutionResult.STANDARD_ATTRIBUTE_STDOUT))
        .isEqualTo("test\n");
  }
}
