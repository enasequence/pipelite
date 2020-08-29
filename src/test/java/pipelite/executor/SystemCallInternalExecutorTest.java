package pipelite.executor;

import org.junit.jupiter.api.Test;
import pipelite.executor.executable.SystemCallInternalExecutor;
import pipelite.instance.TaskInstance;
import pipelite.instance.TaskParameters;
import pipelite.resolver.ResultResolver;
import pipelite.task.TaskExecutionResult;
import pipelite.task.TaskExecutionResultExitCodeSerializer;
import pipelite.task.TaskExecutionResultType;

import static org.assertj.core.api.Assertions.assertThat;

public class SystemCallInternalExecutorTest {

  public static class SuccessTaskExecutor implements TaskExecutor {
    @Override
    public TaskExecutionResult execute(TaskInstance instance) {
      System.out.print("test stdout");
      System.err.print("test stderr");
      return TaskExecutionResult.defaultSuccess();
    }
  }

  public static class PermanentErrorTaskExecutor implements TaskExecutor {
    @Override
    public TaskExecutionResult execute(TaskInstance instance) {
      System.out.print("test stdout");
      System.err.print("test stderr");
      return TaskExecutionResult.defaultPermanentError();
    }
  }

  private TaskInstance taskInstance(TaskExecutor taskExecutor, TaskParameters taskParameters) {

    String processName = "testProcess";
    String processId = "testProcessId";
    String taskName = "testTaskName";

    return TaskInstance.builder()
        .processName(processName)
        .processId(processId)
        .taskName(taskName)
        .executor(taskExecutor)
        .resolver(ResultResolver.DEFAULT_EXCEPTION_RESOLVER)
        .taskParameters(taskParameters)
        .build();
  }

  @Test
  public void testSuccess() {

    SystemCallInternalExecutor executor = new SystemCallInternalExecutor();

    TaskParameters taskParameters = TaskParameters.builder().build();

    TaskExecutor taskExecutor = new SuccessTaskExecutor();

    TaskInstance taskInstance = taskInstance(taskExecutor, taskParameters);

    TaskExecutionResult result = executor.execute(taskInstance);
    assertThat(result.getResultType()).isEqualTo(TaskExecutionResultType.SUCCESS);
    assertThat(result.getAttribute(TaskExecutionResult.STANDARD_ATTRIBUTE_COMMAND))
        .endsWith(
            "pipelite.executor.InternalTaskExecutor "
                + "testProcess "
                + "testProcessId "
                + "testTaskName "
                + "pipelite.executor.SystemCallInternalExecutorTest$SuccessTaskExecutor "
                + "pipelite.resolver.DefaultExceptionResolver");
    assertThat(result.getAttribute(TaskExecutionResult.STANDARD_ATTRIBUTE_STDOUT))
        .contains("test stdout");
    assertThat(result.getAttribute(TaskExecutionResult.STANDARD_ATTRIBUTE_STDERR))
        .contains("test stderr");
    assertThat(result.getAttribute(TaskExecutionResult.STANDARD_ATTRIBUTE_EXIT_CODE))
        .isEqualTo("0");
  }

  @Test
  public void testPermanentError() {

    SystemCallInternalExecutor executor = new SystemCallInternalExecutor();

    TaskParameters taskParameters = TaskParameters.builder().build();

    TaskExecutor taskExecutor = new PermanentErrorTaskExecutor();

    TaskInstance taskInstance = taskInstance(taskExecutor, taskParameters);

    TaskExecutionResult result = executor.execute(taskInstance);
    assertThat(result.getResultType()).isEqualTo(TaskExecutionResultType.PERMANENT_ERROR);
    assertThat(result.getAttribute(TaskExecutionResult.STANDARD_ATTRIBUTE_COMMAND))
        .endsWith(
            "pipelite.executor.InternalTaskExecutor "
                + "testProcess "
                + "testProcessId "
                + "testTaskName "
                + "pipelite.executor.SystemCallInternalExecutorTest$PermanentErrorTaskExecutor "
                + "pipelite.resolver.DefaultExceptionResolver");
    assertThat(result.getAttribute(TaskExecutionResult.STANDARD_ATTRIBUTE_STDOUT))
        .contains("test stdout");
    assertThat(result.getAttribute(TaskExecutionResult.STANDARD_ATTRIBUTE_STDERR))
        .contains("test stderr");
    assertThat(result.getAttribute(TaskExecutionResult.STANDARD_ATTRIBUTE_EXIT_CODE))
        .isEqualTo(
            String.valueOf(
                TaskExecutionResultExitCodeSerializer.EXIT_CODE_DEFAULT_PERMANENT_ERROR));
  }

  @Test
  public void javaMemory() {

    SystemCallInternalExecutor executor = new SystemCallInternalExecutor();

    TaskParameters taskParameters = TaskParameters.builder().memory(2000).build();

    TaskExecutor taskExecutor = new SuccessTaskExecutor();

    TaskInstance taskInstance = taskInstance(taskExecutor, taskParameters);

    TaskExecutionResult result = executor.execute(taskInstance);

    assertThat(result.getAttribute(TaskExecutionResult.STANDARD_ATTRIBUTE_COMMAND))
        .contains(("-Xmx2000M"));
  }

  @Test
  public void testTaskSpecificJavaProperties() {

    SystemCallInternalExecutor executor = new SystemCallInternalExecutor();

    TaskParameters taskParameters =
        TaskParameters.builder()
            .memory(2000)
            .env(new String[] {"PIPELITE_TEST_JAVA_PROPERTY"})
            .build();

    TaskExecutor taskExecutor = new SuccessTaskExecutor();

    TaskInstance taskInstance = taskInstance(taskExecutor, taskParameters);

    TaskExecutionResult result = null;
    try {
      System.setProperty("PIPELITE_TEST_JAVA_PROPERTY", "VALUE");
      result = executor.execute(taskInstance);
    } finally {
      System.clearProperty("PIPELITE_TEST_JAVA_PROPERTY");
    }

    assertThat(result.getAttribute(TaskExecutionResult.STANDARD_ATTRIBUTE_COMMAND))
        .contains(("-DPIPELITE_TEST_JAVA_PROPERTY=VALUE"));
  }
}
