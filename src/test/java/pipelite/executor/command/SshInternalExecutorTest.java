package pipelite.executor.command;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import pipelite.EmptyTestConfiguration;
import pipelite.configuration.SshTestConfiguration;
import pipelite.executor.TaskExecutor;
import pipelite.task.TaskInstance;
import pipelite.task.TaskParameters;
import pipelite.task.TaskExecutionResultExitCode;
import pipelite.task.TaskExecutionResult;
import pipelite.task.TaskExecutionResultType;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(classes = EmptyTestConfiguration.class)
@EnableConfigurationProperties(value = {SshTestConfiguration.class})
@ActiveProfiles("ssh-test")
public class SshInternalExecutorTest {

  @Autowired
  SshTestConfiguration testConfiguration;

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

    SshInternalExecutor executor = new SshInternalExecutor();

    TaskParameters taskParameters = TaskParameters.builder().build();
    taskParameters.setHost(testConfiguration.getHost());

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
                + "'pipelite.executor.command.SshInternalExecutorTest$SuccessTaskExecutor'");
    assertThat(result.getStdout())
        .contains("test stdout");
    assertThat(result.getStderr())
        .contains("test stderr");
    assertThat(result.getAttribute(TaskExecutionResult.EXIT_CODE))
        .isEqualTo("0");
  }

  @Test
  public void testError() {

    SshInternalExecutor executor = new SshInternalExecutor();

    String processName = "testProcess";
    String processId = "testProcessId";
    String taskName = "testTaskName";

    TaskParameters taskParameters = TaskParameters.builder().build();
    taskParameters.setHost(testConfiguration.getHost());

    TaskExecutor taskExecutor = new ErrorTaskExecutor();

    TaskInstance taskInstance =
        TaskInstance.builder()
            .processName(processName)
            .processId(processId)
            .taskName(taskName)
            .executor(taskExecutor)
            .taskParameters(taskParameters)
            .build();

    TaskExecutionResult result = executor.execute(taskInstance);

    assertThat(result.getResultType()).isEqualTo(TaskExecutionResultType.ERROR);
    assertThat(result.getAttribute(TaskExecutionResult.COMMAND))
        .endsWith(
            "'pipelite.executor.InternalExecutor' "
                + "'testProcess' "
                + "'testProcessId' "
                + "'testTaskName' "
                + "'pipelite.executor.command.SshInternalExecutorTest$ErrorTaskExecutor'");
    assertThat(result.getStdout())
        .contains("test stdout");
    assertThat(result.getStderr())
        .contains("test stderr");
    assertThat(result.getAttribute(TaskExecutionResult.EXIT_CODE))
        .isEqualTo(String.valueOf(TaskExecutionResultExitCode.EXIT_CODE_ERROR));
  }

  @Test
  public void javaMemory() {

    SshInternalExecutor executor = new SshInternalExecutor();

    TaskParameters taskParameters = TaskParameters.builder().memory(2000).build();

    TaskExecutor taskExecutor = new LocalInternalExecutorTest.SuccessTaskExecutor();

    TaskInstance taskInstance = taskInstance(taskExecutor, taskParameters);

    TaskExecutionResult result = executor.execute(taskInstance);

    assertThat(result.getAttribute(TaskExecutionResult.COMMAND))
        .contains(("-Xmx2000M"));
  }

  @Test
  public void testTaskSpecificJavaProperties() {

    SshInternalExecutor executor = new SshInternalExecutor();

    TaskParameters taskParameters =
        TaskParameters.builder()
            .memory(2000)
            .env(new String[] {"PIPELITE_TEST_JAVA_PROPERTY"})
            .build();

    TaskExecutor taskExecutor = new LocalInternalExecutorTest.SuccessTaskExecutor();

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
