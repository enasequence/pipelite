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

import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import pipelite.EmptyTestConfiguration;
import pipelite.UniqueStringGenerator;
import pipelite.configuration.LsfTestConfiguration;
import pipelite.task.Task;
import pipelite.task.TaskExecutionResult;
import pipelite.task.TaskExecutionResultType;
import pipelite.task.TaskParameters;

@SpringBootTest(classes = EmptyTestConfiguration.class)
@EnableConfigurationProperties(value = {LsfTestConfiguration.class})
@ActiveProfiles("lsf-test")
public class LsfSshCommandExecutorTest {

  @Autowired LsfTestConfiguration testConfiguration;

  @Test
  public void test() {

    LsfSshCommandExecutor executor = new LsfSshCommandExecutor("echo test");

    String processName = UniqueStringGenerator.randomProcessName();
    String processId = UniqueStringGenerator.randomProcessId();
    String taskName = UniqueStringGenerator.randomTaskName();

    TaskParameters taskParameters =
        TaskParameters.builder()
            .host(testConfiguration.getHost())
            .pollDelay(Duration.ofSeconds(5))
            .build();

    Task task =
        Task.builder()
            .processName(processName)
            .processId(processId)
            .taskName(taskName)
            .executor(executor)
            .taskParameters(taskParameters)
            .build();

    TaskExecutionResult result = executor.execute(task);
    assertThat(result.getResultType()).isEqualTo(TaskExecutionResultType.ACTIVE);
    assertThat(result.getAttribute(TaskExecutionResult.COMMAND)).startsWith("bsub");
    assertThat(result.getAttribute(TaskExecutionResult.COMMAND)).endsWith("echo test");
    assertThat(result.getAttribute(TaskExecutionResult.EXIT_CODE)).isEqualTo("0");
    assertThat(result.getStdout()).contains("is submitted to default queue");

    result = executor.poll(task);
    assertThat(result.getResultType()).isEqualTo(TaskExecutionResultType.SUCCESS);
    assertThat(result.getAttribute(TaskExecutionResult.COMMAND)).isBlank();
    assertThat(result.getAttribute(TaskExecutionResult.EXIT_CODE)).isEqualTo("0");
    assertThat(result.getStdout()).contains("test\n");
  }
}
