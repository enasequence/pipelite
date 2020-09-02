package pipelite.executor.lsf;

import org.junit.jupiter.api.Test;
import pipelite.UniqueStringGenerator;
import pipelite.executor.SuccessTaskExecutor;
import pipelite.resolver.DefaultExitCodeResolver;
import pipelite.resolver.ResultResolver;
import pipelite.task.TaskExecutionResult;
import pipelite.task.TaskInstance;
import pipelite.task.TaskParameters;

import java.io.IOException;
import java.nio.file.Files;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class AbstractLsfExecutorTest {

  private TaskParameters taskParameters() {
    try {
      TaskParameters taskParameters =
          TaskParameters.builder()
              .workDir(Files.createTempDirectory("TEMP").toString())
              .cores(1)
              .memory(1)
              .memoryTimeout(Duration.ofMinutes(1))
              .queue("defaultQueue")
              .build();
      return taskParameters;
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  private TaskInstance taskInstance(TaskParameters taskParameters) {
    return TaskInstance.builder()
        .processName(UniqueStringGenerator.randomProcessName())
        .processId(UniqueStringGenerator.randomProcessId())
        .taskName(UniqueStringGenerator.randomTaskName())
        .executor(new SuccessTaskExecutor())
        .resolver(ResultResolver.DEFAULT_EXCEPTION_RESOLVER)
        .taskParameters(taskParameters)
        .build();
  }

  private static String getCommandline(TaskExecutionResult result) {
    return result.getAttribute(TaskExecutionResult.STANDARD_ATTRIBUTE_COMMAND);
  }

  private AbstractLsfExecutor executor =
      new AbstractLsfExecutor() {
        @Override
        public Call getCall() {
          return (cmd, taskParameters) -> new CallResult(0, "Job <13454> is submitted", "");
        }

        @Override
        public String getCmd(TaskInstance taskInstance) {
          return "echo test";
        }

        @Override
        public Resolver getResolver() {
          return (taskInstance, exitCode) -> new DefaultExitCodeResolver().resolve(exitCode);
        }
      };

  @Test
  public void testCmd() {
    TaskParameters taskParameters = taskParameters();

    String cmd = getCommandline(executor.execute(taskInstance(taskParameters)));
    assertTrue(cmd.contains(" -M 1 -R rusage[mem=1:duration=1]"));
    assertTrue(cmd.contains(" -n 1"));
    assertTrue(cmd.contains(" -q defaultQueue"));
    assertTrue(cmd.contains(" -oo " + taskParameters.getWorkDir()));
    assertTrue(cmd.contains(" -eo " + taskParameters.getWorkDir()));
  }

  @Test
  public void testCmdNoQueue() {
    TaskParameters taskParameters = taskParameters();
    taskParameters.setQueue(null);

    String cmd = getCommandline(executor.execute(taskInstance(taskParameters)));
    assertFalse(cmd.contains("-q "));
  }

  @Test
  public void testCmdQueue() {
    TaskParameters taskParameters = taskParameters();
    taskParameters.setQueue("queue");

    String cmd = getCommandline(executor.execute(taskInstance(taskParameters)));
    assertTrue(cmd.contains("-q queue"));
  }

  @Test
  public void testCmdMemoryAndCores() {
    TaskParameters taskParameters = taskParameters();
    taskParameters.setMemory(2000);
    taskParameters.setCores(12);

    String cmd = getCommandline(executor.execute(taskInstance(taskParameters)));
    assertTrue(cmd.contains(" -M 2000 -R rusage[mem=2000:duration=1]"));
    assertTrue(cmd.contains(" -n 12"));
    assertTrue(cmd.contains(" -q defaultQueue"));
    assertTrue(cmd.contains(" -oo " + taskParameters.getWorkDir()));
    assertTrue(cmd.contains(" -eo " + taskParameters.getWorkDir()));
  }

  @Test
  public void testExtractJobIdSubmitted() {
    assertThat(
            AbstractLsfExecutor.extractJobIdSubmitted(
                "Job <2848143> is submitted to default queue <research-rh74>."))
        .isEqualTo("2848143");

    assertThat(AbstractLsfExecutor.extractJobIdSubmitted("Job <2848143> is submitted "))
        .isEqualTo("2848143");
  }

  @Test
  public void testExtractJobIdNotFound() {
    assertThat(AbstractLsfExecutor.extractJobIdNotFound("Job <345654> is not found.")).isTrue();
    assertThat(AbstractLsfExecutor.extractJobIdNotFound("Job <345654> is not found")).isTrue();
    assertThat(AbstractLsfExecutor.extractJobIdNotFound("Job <345654> is ")).isFalse();
  }

  @Test
  public void testExtractExitCode() {
    assertThat(AbstractLsfExecutor.extractExitCode("Exited with exit code 1")).isEqualTo("1");
    assertThat(AbstractLsfExecutor.extractExitCode("Exited with exit code 3.")).isEqualTo("3");
  }
}
