package pipelite.executor;

import org.junit.jupiter.api.Test;
import pipelite.UniqueStringGenerator;
import pipelite.task.TaskInstance;
import pipelite.task.TaskParameters;
import pipelite.resolver.ResultResolver;
import pipelite.task.TaskExecutionResult;

import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

public class InternalExecutorTest {

  @Test
  public void test() {

    String processName = UniqueStringGenerator.randomProcessName();
    String processId = UniqueStringGenerator.randomProcessId();
    String taskName = UniqueStringGenerator.randomTaskName();

    AtomicInteger taskExecutionCount = new AtomicInteger();

    InternalExecutor internalExecutor = new InternalExecutor();

    TaskExecutor taskExecutor =
        taskInstance -> {
          taskExecutionCount.getAndIncrement();
          return TaskExecutionResult.defaultSuccess();
        };

    TaskParameters taskParameters = TaskParameters.builder().build();

    TaskInstance taskInstance =
        TaskInstance.builder()
            .processName(processName)
            .processId(processId)
            .taskName(taskName)
            .executor(taskExecutor)
            .resolver(ResultResolver.DEFAULT_EXCEPTION_RESOLVER)
            .taskParameters(taskParameters)
            .build();

    TaskExecutionResult result = internalExecutor.execute(taskInstance);
    assertThat(result).isEqualTo(TaskExecutionResult.defaultSuccess());
    assertThat(taskExecutionCount.get()).isEqualTo(1);
  }
}
