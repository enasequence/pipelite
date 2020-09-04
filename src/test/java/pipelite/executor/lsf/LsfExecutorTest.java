package pipelite.executor.lsf;

import org.junit.jupiter.api.Test;
import pipelite.UniqueStringGenerator;
import pipelite.executor.SuccessTaskExecutor;
import pipelite.executor.runner.CommandRunner;
import pipelite.executor.runner.CommandRunnerResult;
import pipelite.task.TaskExecutionResult;
import pipelite.task.TaskInstance;
import pipelite.task.TaskParameters;

import java.io.IOException;
import java.nio.file.Files;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class LsfExecutorTest {

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
        .taskParameters(taskParameters)
        .build();
  }

  private static String getCommandline(TaskExecutionResult result) {
    return result.getAttribute(TaskExecutionResult.COMMAND);
  }

  private LsfExecutor executor =
      new LsfExecutor() {
        @Override
        public CommandRunner getCmdRunner() {
          return (cmd, taskParameters) -> new CommandRunnerResult(0, "Job <13454> is submitted", "");
        }

        @Override
        public String getCmd(TaskInstance taskInstance) {
          return "echo test";
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
            LsfExecutor.extractJobIdSubmitted(
                "Job <2848143> is submitted to default queue <research-rh74>."))
        .isEqualTo("2848143");

    assertThat(LsfExecutor.extractJobIdSubmitted("Job <2848143> is submitted "))
        .isEqualTo("2848143");
  }

  @Test
  public void testExtractJobIdNotFound() {
    assertThat(LsfExecutor.extractJobIdNotFound("Job <345654> is not found.")).isTrue();
    assertThat(LsfExecutor.extractJobIdNotFound("Job <345654> is not found")).isTrue();
    assertThat(LsfExecutor.extractJobIdNotFound("Job <345654> is ")).isFalse();
  }

  @Test
  public void testExtractExitCode() {
    assertThat(LsfExecutor.extractExitCode("Exited with exit code 1")).isEqualTo("1");
    assertThat(LsfExecutor.extractExitCode("Exited with exit code 3.")).isEqualTo("3");
  }
}