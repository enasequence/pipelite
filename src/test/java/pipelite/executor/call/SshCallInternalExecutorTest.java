package pipelite.executor.call;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import pipelite.EmptyTestConfiguration;
import pipelite.configuration.TestConfiguration;
import pipelite.executor.TaskExecutor;
import pipelite.task.TaskInstance;
import pipelite.task.TaskParameters;
import pipelite.resolver.ResultResolver;
import pipelite.task.TaskExecutionResult;
import pipelite.task.TaskExecutionResultExitCodeSerializer;
import pipelite.task.TaskExecutionResultType;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(classes = EmptyTestConfiguration.class)
@EnableConfigurationProperties(value = {TestConfiguration.class})
@ActiveProfiles("test")
public class SshCallInternalExecutorTest {

  @Autowired TestConfiguration testConfiguration;

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
        .resolver(ResultResolver.DEFAULT_EXCEPTION_RESOLVER)
        .taskParameters(taskParameters)
        .build();
  }

  @Test
  public void testSuccess() {

    SshCallInternalExecutor executor = new SshCallInternalExecutor();

    TaskParameters taskParameters = TaskParameters.builder().build();
    taskParameters.setHost(testConfiguration.getSsh().getHost());

    TaskExecutor taskExecutor = new SuccessTaskExecutor();

    TaskInstance taskInstance = taskInstance(taskExecutor, taskParameters);

    TaskExecutionResult result = executor.execute(taskInstance);
    assertThat(result.getResultType()).isEqualTo(TaskExecutionResultType.SUCCESS);
    assertThat(result.getAttribute(TaskExecutionResult.STANDARD_ATTRIBUTE_COMMAND))
        .endsWith(
            "'pipelite.executor.InternalExecutor' "
                + "'testProcess' "
                + "'testProcessId' "
                + "'testTaskName' "
                + "'pipelite.executor.call.SshCallInternalExecutorTest$SuccessTaskExecutor' "
                + "'pipelite.resolver.DefaultExceptionResolver'");
    assertThat(result.getAttribute(TaskExecutionResult.STANDARD_ATTRIBUTE_STDOUT))
        .contains("test stdout");
    assertThat(result.getAttribute(TaskExecutionResult.STANDARD_ATTRIBUTE_STDERR))
        .contains("test stderr");
    assertThat(result.getAttribute(TaskExecutionResult.STANDARD_ATTRIBUTE_EXIT_CODE))
        .isEqualTo("0");
  }

  @Test
  public void testError() {

    SshCallInternalExecutor executor = new SshCallInternalExecutor();

    String processName = "testProcess";
    String processId = "testProcessId";
    String taskName = "testTaskName";

    TaskParameters taskParameters = TaskParameters.builder().build();
    taskParameters.setHost(testConfiguration.getSsh().getHost());

    TaskExecutor taskExecutor = new ErrorTaskExecutor();

    TaskInstance taskInstance =
        TaskInstance.builder()
            .processName(processName)
            .processId(processId)
            .taskName(taskName)
            .executor(taskExecutor)
            .resolver(ResultResolver.DEFAULT_EXCEPTION_RESOLVER)
            .taskParameters(taskParameters)
            .build();

    TaskExecutionResult result = executor.execute(taskInstance);

    assertThat(result.getResultType()).isEqualTo(TaskExecutionResultType.ERROR);
    assertThat(result.getAttribute(TaskExecutionResult.STANDARD_ATTRIBUTE_COMMAND))
        .endsWith(
            "'pipelite.executor.InternalExecutor' "
                + "'testProcess' "
                + "'testProcessId' "
                + "'testTaskName' "
                + "'pipelite.executor.call.SshCallInternalExecutorTest$ErrorTaskExecutor' "
                + "'pipelite.resolver.DefaultExceptionResolver'");
    assertThat(result.getAttribute(TaskExecutionResult.STANDARD_ATTRIBUTE_STDOUT))
        .contains("test stdout");
    assertThat(result.getAttribute(TaskExecutionResult.STANDARD_ATTRIBUTE_STDERR))
        .contains("test stderr");
    assertThat(result.getAttribute(TaskExecutionResult.STANDARD_ATTRIBUTE_EXIT_CODE))
        .isEqualTo(String.valueOf(TaskExecutionResultExitCodeSerializer.EXIT_CODE_ERROR));
  }

  @Test
  public void javaMemory() {

    SshCallInternalExecutor executor = new SshCallInternalExecutor();

    TaskParameters taskParameters = TaskParameters.builder().memory(2000).build();

    TaskExecutor taskExecutor = new SystemCallInternalExecutorTest.SuccessTaskExecutor();

    TaskInstance taskInstance = taskInstance(taskExecutor, taskParameters);

    TaskExecutionResult result = executor.execute(taskInstance);

    assertThat(result.getAttribute(TaskExecutionResult.STANDARD_ATTRIBUTE_COMMAND))
        .contains(("-Xmx2000M"));
  }

  @Test
  public void testTaskSpecificJavaProperties() {

    SshCallInternalExecutor executor = new SshCallInternalExecutor();

    TaskParameters taskParameters =
        TaskParameters.builder()
            .memory(2000)
            .env(new String[] {"PIPELITE_TEST_JAVA_PROPERTY"})
            .build();

    TaskExecutor taskExecutor = new SystemCallInternalExecutorTest.SuccessTaskExecutor();

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
