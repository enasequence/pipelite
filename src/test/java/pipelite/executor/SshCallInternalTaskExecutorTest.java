package pipelite.executor;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import pipelite.EmptyTestConfiguration;
import pipelite.configuration.TestConfiguration;
import pipelite.instance.TaskInstance;
import pipelite.instance.TaskParameters;
import pipelite.resolver.ResultResolver;
import pipelite.task.TaskExecutionResult;
import pipelite.task.TaskExecutionResultExitCodeSerializer;
import pipelite.task.TaskExecutionResultType;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(classes = EmptyTestConfiguration.class)
@EnableConfigurationProperties(value = {TestConfiguration.class})
@ActiveProfiles("test")
public class SshCallInternalTaskExecutorTest {

  @Autowired
  TestConfiguration testConfiguration;

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

    SshCallInternalTaskExecutor sshCallInternalTaskExecutor =
        SshCallInternalTaskExecutor.builder().build();

    String processName = "testProcess";
    String processId = "testProcessId";
    String taskName = "testTaskName";

    TaskParameters taskParameters = TaskParameters.builder().build();
    taskParameters.setHost(testConfiguration.getSsh().getHost());

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

    TaskExecutionResult result = sshCallInternalTaskExecutor.execute(taskInstance);
    assertThat(result.getResultType()).isEqualTo(TaskExecutionResultType.SUCCESS);
    assertThat(result.getAttribute(TaskExecutionResult.STANDARD_ATTRIBUTE_COMMAND))
        .endsWith(
            "pipelite.executor.InternalTaskExecutor "
                + "'testProcess' "
                + "'testProcessId' "
                + "'testTaskName' "
                + "'pipelite.executor.SshCallInternalTaskExecutorTest$SuccessTaskExecutor' "
                + "'pipelite.resolver.DefaultExceptionResolver'");
    assertThat(result.getAttribute(TaskExecutionResult.STANDARD_ATTRIBUTE_STDOUT))
        .contains("test stdout");
    assertThat(result.getAttribute(TaskExecutionResult.STANDARD_ATTRIBUTE_STDERR))
        .contains("test stderr");
    assertThat(result.getAttribute(TaskExecutionResult.STANDARD_ATTRIBUTE_EXIT_CODE))
        .isEqualTo("0");
  }

  @Test
  public void testPermanentError() {

    SshCallInternalTaskExecutor sshCallInternalTaskExecutor =
        SshCallInternalTaskExecutor.builder().build();

    String processName = "testProcess";
    String processId = "testProcessId";
    String taskName = "testTaskName";

    TaskParameters taskParameters = TaskParameters.builder().build();
    taskParameters.setHost(testConfiguration.getSsh().getHost());

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

    TaskExecutionResult result = sshCallInternalTaskExecutor.execute(taskInstance);

    assertThat(result.getResultType()).isEqualTo(TaskExecutionResultType.PERMANENT_ERROR);
    assertThat(result.getAttribute(TaskExecutionResult.STANDARD_ATTRIBUTE_COMMAND))
        .endsWith(
            "pipelite.executor.InternalTaskExecutor "
                + "'testProcess' "
                + "'testProcessId' "
                + "'testTaskName' "
                + "'pipelite.executor.SshCallInternalTaskExecutorTest$PermanentErrorTaskExecutor' "
                + "'pipelite.resolver.DefaultExceptionResolver'");
    assertThat(result.getAttribute(TaskExecutionResult.STANDARD_ATTRIBUTE_STDOUT))
        .contains("test stdout");
    assertThat(result.getAttribute(TaskExecutionResult.STANDARD_ATTRIBUTE_STDERR))
        .contains("test stderr");
    assertThat(result.getAttribute(TaskExecutionResult.STANDARD_ATTRIBUTE_EXIT_CODE))
        .isEqualTo(String.valueOf(TaskExecutionResultExitCodeSerializer.EXIT_CODE_DEFAULT_PERMANENT_ERROR));
  }
}
