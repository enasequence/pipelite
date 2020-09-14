/*
 * Copyright 2020 EMBL - European Bioinformatics Institute
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package pipelite.executor.lsf;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import pipelite.EmptyTestConfiguration;
import pipelite.UniqueStringGenerator;
import pipelite.configuration.SshTestConfiguration;
import pipelite.executor.SuccessTaskExecutor;
import pipelite.executor.command.LocalCommandExecutor;
import pipelite.executor.command.SshCommandExecutor;
import pipelite.executor.runner.CommandRunner;
import pipelite.executor.runner.CommandRunnerResult;
import pipelite.task.Task;
import pipelite.task.TaskExecutionResult;
import pipelite.task.TaskExecutionResultType;
import pipelite.task.TaskParameters;

@SpringBootTest(classes = EmptyTestConfiguration.class)
@EnableConfigurationProperties(value = {SshTestConfiguration.class})
@ActiveProfiles("ssh-test")
public class LsfExecutorTest {

  @Autowired private SshTestConfiguration testConfiguration;

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

  private Task task(TaskParameters taskParameters) {
    return Task.builder()
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
          return (cmd, taskParameters) ->
              new CommandRunnerResult(0, "Job <13454> is submitted", "");
        }

        @Override
        public String getCmd(Task task) {
          return "echo test";
        }
      };

  @Test
  public void testStdoutWithLocalExecutor() throws IOException {
    Task task = Task.builder().build();

    File file = File.createTempFile("pipellite-test", "");
    file.createNewFile();

    LocalCommandExecutor executor =
        new LocalCommandExecutor("sh -c 'echo test > " + file.getAbsolutePath() + "'");
    TaskExecutionResult result = executor.execute(task);
    assertThat(result.getResultType()).isEqualTo(TaskExecutionResultType.SUCCESS);
    CommandRunnerResult runnerResult =
        LsfExecutor.writeFileToStdout(executor.getCmdRunner(), file.getAbsolutePath(), task);
    assertThat(runnerResult.getStdout()).isEqualTo("test\n");
  }

  @Test
  public void testStderrWithLocalExecutor() throws IOException {
    Task task = Task.builder().build();

    File file = File.createTempFile("pipellite-test", "");
    file.createNewFile();

    LocalCommandExecutor executor =
        new LocalCommandExecutor("sh -c 'echo test > " + file.getAbsolutePath() + "'");
    TaskExecutionResult result = executor.execute(task);
    assertThat(result.getResultType()).isEqualTo(TaskExecutionResultType.SUCCESS);
    CommandRunnerResult runnerResult =
        LsfExecutor.writeFileToStderr(executor.getCmdRunner(), file.getAbsolutePath(), task);
    assertThat(runnerResult.getStderr()).isEqualTo("test\n");
  }

  @Test
  public void testStdoutWithSshExecutor() throws IOException {
    Task task = Task.builder().build();
    task.getTaskParameters().setHost(testConfiguration.getHost());

    File file = File.createTempFile("pipellite-test", "");
    file.createNewFile();

    SshCommandExecutor executor =
        new SshCommandExecutor("sh -c 'echo test > " + file.getAbsolutePath() + "'");
    TaskExecutionResult result = executor.execute(task);
    assertThat(result.getResultType()).isEqualTo(TaskExecutionResultType.SUCCESS);
    CommandRunnerResult runnerResult =
        LsfExecutor.writeFileToStdout(executor.getCmdRunner(), file.getAbsolutePath(), task);
    assertThat(runnerResult.getStdout()).isEqualTo("test\n");
  }

  @Test
  public void testStderrWithSshExecutor() throws IOException {
    Task task = Task.builder().build();
    task.getTaskParameters().setHost(testConfiguration.getHost());

    File file = File.createTempFile("pipellite-test", "");
    file.createNewFile();

    SshCommandExecutor executor =
        new SshCommandExecutor("sh -c 'echo test > " + file.getAbsolutePath() + "'");

    TaskExecutionResult result = executor.execute(task);
    assertThat(result.getResultType()).isEqualTo(TaskExecutionResultType.SUCCESS);
    CommandRunnerResult runnerResult =
        LsfExecutor.writeFileToStderr(executor.getCmdRunner(), file.getAbsolutePath(), task);
    assertThat(runnerResult.getStderr()).isEqualTo("test\n");
  }

  @Test
  public void testCmdArguments() {
    TaskParameters taskParameters = taskParameters();

    String cmd = getCommandline(executor.execute(task(taskParameters)));
    assertTrue(cmd.contains(" -M 1 -R rusage[mem=1:duration=1]"));
    assertTrue(cmd.contains(" -n 1"));
    assertTrue(cmd.contains(" -q defaultQueue"));
    assertTrue(cmd.contains(" -oo " + taskParameters.getWorkDir()));
    assertTrue(cmd.contains(" -eo " + taskParameters.getWorkDir()));
  }

  @Test
  public void testNoQueueCmdArgument() {
    TaskParameters taskParameters = taskParameters();
    taskParameters.setQueue(null);

    String cmd = getCommandline(executor.execute(task(taskParameters)));
    assertFalse(cmd.contains("-q "));
  }

  @Test
  public void testQueueCmdArgument() {
    TaskParameters taskParameters = taskParameters();
    taskParameters.setQueue("queue");

    String cmd = getCommandline(executor.execute(task(taskParameters)));
    assertTrue(cmd.contains("-q queue"));
  }

  @Test
  public void testMemoryAndCoresCmdArgument() {
    TaskParameters taskParameters = taskParameters();
    taskParameters.setMemory(2000);
    taskParameters.setCores(12);

    String cmd = getCommandline(executor.execute(task(taskParameters)));
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
