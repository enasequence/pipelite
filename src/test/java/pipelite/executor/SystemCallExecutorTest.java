package pipelite.executor;

import org.junit.jupiter.api.Test;
import pipelite.UniqueStringGenerator;
import pipelite.instance.TaskInstance;
import pipelite.instance.TaskParameters;
import pipelite.resolver.DefaultExceptionResolver;
import pipelite.resolver.DefaultExitCodeResolver;
import pipelite.task.TaskExecutionResult;
import pipelite.task.TaskExecutionResultType;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

public class SystemCallExecutorTest {

  private static AtomicInteger taskExecutionCount = new AtomicInteger();

  private static class EchoSystemCall extends SystemCallTaskExecutor {
    @Override
    protected String getExecutable() {
      return "echo";
    }

    @Override
    protected Collection<String> getArguments() {
      return Arrays.asList("test");
    }

    @Override
    public TaskExecutionResult execute(TaskInstance taskInstance) {
      taskExecutionCount.incrementAndGet();
      return super.execute(taskInstance);
    }
  }

  @Test
  public void test() {

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
            .resolver(new DefaultExitCodeResolver())
            .taskParameters(taskParameters)
            .build();

    TaskExecutor taskExecutor = new EchoSystemCall();

    TaskExecutionResult result = taskExecutor.execute(taskInstance);
    assertThat(result.getResultType()).isEqualTo(TaskExecutionResultType.SUCCESS);
    assertThat(taskExecutionCount.get()).isEqualTo(1);
    assertThat(result.getAttribute(TaskExecutionResult.STANDARD_ATTRIBUTE_COMMAND))
        .isEqualTo("echo test");
    assertThat(result.getAttribute(TaskExecutionResult.STANDARD_ATTRIBUTE_EXIT_CODE))
        .isEqualTo("0");
    assertThat(result.getAttribute(TaskExecutionResult.STANDARD_ATTRIBUTE_STDOUT))
        .isEqualTo("test\n");
  }
}
