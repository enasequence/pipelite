package pipelite.executor;

import org.junit.jupiter.api.Test;
import pipelite.UniqueStringGenerator;
import pipelite.instance.TaskInstance;
import pipelite.instance.TaskParameters;
import pipelite.resolver.DefaultExceptionResolver;
import pipelite.task.TaskExecutionResult;
import uk.ac.ebi.ena.sra.pipeline.launcher.InternalTaskExecutor;

import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

public class InternalTaskExecutorTest {

  @Test
  public void test() {

    String processName = UniqueStringGenerator.randomProcessName();
    String processId = UniqueStringGenerator.randomProcessId();
    String taskName = UniqueStringGenerator.randomTaskName();

    AtomicInteger taskExecutionCount = new AtomicInteger();

    InternalTaskExecutor internalTaskExecutor = new InternalTaskExecutor();

    TaskExecutor taskExecutor =
        taskInstance -> {
          taskExecutionCount.getAndIncrement();
          return TaskExecutionResult.success();
        };

    TaskParameters taskParameters = TaskParameters.builder().build();

    TaskInstance taskInstance =
        TaskInstance.builder()
            .processName(processName)
            .processId(processId)
            .taskName(taskName)
            .executor(taskExecutor)
            .resolver(new DefaultExceptionResolver())
            .taskParameters(taskParameters)
            .build();

    TaskExecutionResult result = internalTaskExecutor.execute(taskInstance);
    assertThat(result).isEqualTo(TaskExecutionResult.success());
    assertThat(taskExecutionCount.get()).isEqualTo(1);
  }
}
