package pipelite.executor.call;

import org.junit.jupiter.api.Test;
import pipelite.executor.TaskExecutor;
import pipelite.task.TaskInstance;
import pipelite.task.TaskParameters;
import pipelite.task.TaskExecutionResultExitCode;
import pipelite.task.TaskExecutionResult;
import pipelite.task.TaskExecutionResultType;

import static org.assertj.core.api.Assertions.assertThat;

public class SystemCallInternalExecutorTest {

  public static class SuccessTaskExecutor implements TaskExecutor {
    @Override
    public TaskExecutionResult execute(TaskInstance taskInstance) {
      System.out.print("test stdout");
      System.err.print("test stderr");
      return TaskExecutionResult.success();
    }
  }

  public static class ErrorTaskExecutor implements TaskExecutor {
    @Override
    public TaskExecutionResult execute(TaskInstance taskInstance) {
      System.out.print("test stdout");
      System.err.print("test stderr");
      return TaskExecutionResult.error();
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
    assertThat(result.getAttribute(TaskExecutionResult.COMMAND))
        .endsWith(
            "'pipelite.executor.InternalExecutor' "
                + "'testProcess' "
                + "'testProcessId' "
                + "'testTaskName' "
                + "'pipelite.executor.call.SystemCallInternalExecutorTest$SuccessTaskExecutor'");
    assertThat(result.getStdout())
        .contains("test stdout");
    assertThat(result.getStderr())
        .contains("test stderr");
    assertThat(result.getAttribute(TaskExecutionResult.EXIT_CODE))
        .isEqualTo("0");
  }

  @Test
  public void testError() {

    SystemCallInternalExecutor executor = new SystemCallInternalExecutor();

    TaskParameters taskParameters = TaskParameters.builder().build();

    TaskExecutor taskExecutor = new ErrorTaskExecutor();

    TaskInstance taskInstance = taskInstance(taskExecutor, taskParameters);

    TaskExecutionResult result = executor.execute(taskInstance);
    assertThat(result.getResultType()).isEqualTo(TaskExecutionResultType.ERROR);
    assertThat(result.getAttribute(TaskExecutionResult.COMMAND))
        .endsWith(
            "'pipelite.executor.InternalExecutor' "
                + "'testProcess' "
                + "'testProcessId' "
                + "'testTaskName' "
                + "'pipelite.executor.call.SystemCallInternalExecutorTest$ErrorTaskExecutor'");
    assertThat(result.getStdout())
        .contains("test stdout");
    assertThat(result.getStderr())
        .contains("test stderr");
    assertThat(result.getAttribute(TaskExecutionResult.EXIT_CODE))
        .isEqualTo(String.valueOf(TaskExecutionResultExitCode.EXIT_CODE_ERROR));
  }

  @Test
  public void javaMemory() {

    SystemCallInternalExecutor executor = new SystemCallInternalExecutor();

    TaskParameters taskParameters = TaskParameters.builder().memory(2000).build();

    TaskExecutor taskExecutor = new SuccessTaskExecutor();

    TaskInstance taskInstance = taskInstance(taskExecutor, taskParameters);

    TaskExecutionResult result = executor.execute(taskInstance);

    assertThat(result.getAttribute(TaskExecutionResult.COMMAND))
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

    assertThat(result.getAttribute(TaskExecutionResult.COMMAND))
        .contains(("-DPIPELITE_TEST_JAVA_PROPERTY=VALUE"));
  }
}
