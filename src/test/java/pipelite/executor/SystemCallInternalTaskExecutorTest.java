package pipelite.executor;

import org.junit.jupiter.api.Test;
import pipelite.instance.TaskInstance;
import pipelite.instance.TaskParameters;
import pipelite.resolver.ResultResolver;
import pipelite.task.TaskExecutionResult;
import pipelite.task.TaskExecutionResultExitCodeSerializer;
import pipelite.task.TaskExecutionResultType;

import static org.assertj.core.api.Assertions.assertThat;

public class SystemCallInternalTaskExecutorTest {

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

  @Test
  public void testSuccess() {

    SystemCallInternalTaskExecutor systemCallInternalTaskExecutor =
        SystemCallInternalTaskExecutor.builder().build();

    String processName = "testProcess";
    String processId = "testProcessId";
    String taskName = "testTaskName";

    TaskParameters taskParameters = TaskParameters.builder().build();

    TaskExecutor taskExecutor = new SuccessTaskExecutor();

    TaskInstance taskInstance =
        TaskInstance.builder()
            .processName(processName)
            .processId(processId)
            .taskName(taskName)
            .executor(taskExecutor)
            .resolver(ResultResolver.DEFAULT_EXCEPTION_RESOLVER)
            .taskParameters(taskParameters)
            .build();

    TaskExecutionResult result = systemCallInternalTaskExecutor.execute(taskInstance);
    assertThat(result.getResultType()).isEqualTo(TaskExecutionResultType.SUCCESS);
    assertThat(result.getAttribute(TaskExecutionResult.STANDARD_ATTRIBUTE_COMMAND))
        .endsWith(
            "uk.ac.ebi.ena.sra.pipeline.launcher.InternalTaskExecutor "
                + "testProcess "
                + "testProcessId "
                + "testTaskName "
                + "pipelite.executor.SystemCallInternalTaskExecutorTest$SuccessTaskExecutor "
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

    SystemCallInternalTaskExecutor systemCallInternalTaskExecutor =
        SystemCallInternalTaskExecutor.builder().build();

    String processName = "testProcess";
    String processId = "testProcessId";
    String taskName = "testTaskName";

    TaskParameters taskParameters = TaskParameters.builder().build();

    TaskExecutor taskExecutor = new PermanentErrorTaskExecutor();

    TaskInstance taskInstance =
        TaskInstance.builder()
            .processName(processName)
            .processId(processId)
            .taskName(taskName)
            .executor(taskExecutor)
            .resolver(ResultResolver.DEFAULT_EXCEPTION_RESOLVER)
            .taskParameters(taskParameters)
            .build();

    TaskExecutionResult result = systemCallInternalTaskExecutor.execute(taskInstance);
    assertThat(result.getResultType()).isEqualTo(TaskExecutionResultType.PERMANENT_ERROR);
    assertThat(result.getAttribute(TaskExecutionResult.STANDARD_ATTRIBUTE_COMMAND))
        .endsWith(
            "uk.ac.ebi.ena.sra.pipeline.launcher.InternalTaskExecutor "
                + "testProcess "
                + "testProcessId "
                + "testTaskName "
                + "pipelite.executor.SystemCallInternalTaskExecutorTest$PermanentErrorTaskExecutor "
                + "pipelite.resolver.DefaultExceptionResolver");
    assertThat(result.getAttribute(TaskExecutionResult.STANDARD_ATTRIBUTE_STDOUT))
        .contains("test stdout");
    assertThat(result.getAttribute(TaskExecutionResult.STANDARD_ATTRIBUTE_STDERR))
        .contains("test stderr");
    assertThat(result.getAttribute(TaskExecutionResult.STANDARD_ATTRIBUTE_EXIT_CODE))
        .isEqualTo(String.valueOf(TaskExecutionResultExitCodeSerializer.EXIT_CODE_DEFAULT_PERMANENT_ERROR));
  }
}
