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
package pipelite.executor.command;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import pipelite.EmptyTestConfiguration;
import pipelite.configuration.SshTestConfiguration;
import pipelite.executor.TaskExecutor;
import pipelite.task.TaskExecutionResult;
import pipelite.task.TaskExecutionResultExitCode;
import pipelite.task.TaskExecutionResultType;
import pipelite.task.TaskInstance;
import pipelite.task.TaskParameters;

@SpringBootTest(classes = EmptyTestConfiguration.class)
@EnableConfigurationProperties(value = {SshTestConfiguration.class})
@ActiveProfiles("ssh-test")
public class SshTaskExecutorTest {

  @Autowired SshTestConfiguration testConfiguration;

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

  private TaskInstance taskInstance(TaskExecutor executor, TaskParameters taskParameters) {

    String processName = "testProcess";
    String processId = "testProcessId";
    String taskName = "testTaskName";

    return TaskInstance.builder()
        .processName(processName)
        .processId(processId)
        .taskName(taskName)
        .executor(executor)
        .taskParameters(taskParameters)
        .build();
  }

  @Test
  public void testSuccess() {

    TaskParameters taskParameters = TaskParameters.builder().build();
    taskParameters.setHost(testConfiguration.getHost());

    TaskInstance taskInstance =
        taskInstance(new SshTaskExecutor(new SuccessTaskExecutor()), taskParameters);

    TaskExecutionResult result = taskInstance.execute();
    assertThat(result.getResultType()).isEqualTo(TaskExecutionResultType.SUCCESS);
    assertThat(result.getAttribute(TaskExecutionResult.COMMAND))
        .endsWith(
            "'pipelite.executor.InternalExecutor' "
                + "'testProcess' "
                + "'testProcessId' "
                + "'testTaskName' "
                + "'pipelite.executor.command.SshTaskExecutorTest$SuccessTaskExecutor'");
    assertThat(result.getStdout()).contains("test stdout");
    assertThat(result.getStderr()).contains("test stderr");
    assertThat(result.getAttribute(TaskExecutionResult.EXIT_CODE)).isEqualTo("0");
  }

  @Test
  public void testError() {

    String processName = "testProcess";
    String processId = "testProcessId";
    String taskName = "testTaskName";

    TaskParameters taskParameters = TaskParameters.builder().build();
    taskParameters.setHost(testConfiguration.getHost());

    TaskInstance taskInstance =
        TaskInstance.builder()
            .processName(processName)
            .processId(processId)
            .taskName(taskName)
            .executor(new SshTaskExecutor(new ErrorTaskExecutor()))
            .taskParameters(taskParameters)
            .build();

    TaskExecutionResult result = taskInstance.execute();

    assertThat(result.getResultType()).isEqualTo(TaskExecutionResultType.ERROR);
    assertThat(result.getAttribute(TaskExecutionResult.COMMAND))
        .endsWith(
            "'pipelite.executor.InternalExecutor' "
                + "'testProcess' "
                + "'testProcessId' "
                + "'testTaskName' "
                + "'pipelite.executor.command.SshTaskExecutorTest$ErrorTaskExecutor'");
    assertThat(result.getStdout()).contains("test stdout");
    assertThat(result.getStderr()).contains("test stderr");
    assertThat(result.getAttribute(TaskExecutionResult.EXIT_CODE))
        .isEqualTo(String.valueOf(TaskExecutionResultExitCode.EXIT_CODE_ERROR));
  }

  @Test
  public void javaMemory() {

    SshTaskExecutor executor = new SshTaskExecutor(new SuccessTaskExecutor());

    TaskParameters taskParameters = TaskParameters.builder().memory(2000).build();

    TaskInstance taskInstance = taskInstance(executor, taskParameters);

    TaskExecutionResult result = executor.execute(taskInstance);

    assertThat(result.getAttribute(TaskExecutionResult.COMMAND)).contains(("-Xmx2000M"));
  }

  @Test
  public void testTaskSpecificJavaProperties() {

    SshTaskExecutor executor = new SshTaskExecutor(new SuccessTaskExecutor());

    TaskParameters taskParameters =
        TaskParameters.builder()
            .memory(2000)
            .env(new String[] {"PIPELITE_TEST_JAVA_PROPERTY"})
            .build();

    TaskInstance taskInstance = taskInstance(executor, taskParameters);

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
