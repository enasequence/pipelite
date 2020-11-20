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
package pipelite.executor.cmd;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import pipelite.PipeliteTestConfiguration;
import pipelite.UniqueStringGenerator;
import pipelite.configuration.LsfTestConfiguration;
import pipelite.executor.StageExecutor;
import pipelite.stage.Stage;
import pipelite.stage.StageExecutionResult;
import pipelite.stage.StageExecutionResultType;
import pipelite.stage.StageParameters;

@SpringBootTest(classes = PipeliteTestConfiguration.class)
@ActiveProfiles("test")
public class LsfSshCmdExecutorTest {

  @Autowired LsfTestConfiguration lsfTestConfiguration;

  @Test
  // @Timeout(value = 60, unit = TimeUnit.SECONDS)
  public void test() {

    LsfCmdExecutor executor = StageExecutor.createLsfSshCmdExecutor("echo test");

    String pipelineName = UniqueStringGenerator.randomPipelineName();
    String processId = UniqueStringGenerator.randomProcessId();
    String stageName = UniqueStringGenerator.randomStageName();

    StageParameters stageParameters =
        StageParameters.builder()
            .host(lsfTestConfiguration.getHost())
            .pollFrequency(Duration.ofSeconds(5))
            .build();

    Stage stage =
        Stage.builder()
            .pipelineName(pipelineName)
            .processId(processId)
            .stageName(stageName)
            .executor(executor)
            .stageParameters(stageParameters)
            .build();

    StageExecutionResult result = executor.execute(stage);
    assertThat(result.getResultType()).isEqualTo(StageExecutionResultType.ACTIVE);
    assertThat(result.getAttribute(StageExecutionResult.COMMAND)).startsWith("bsub");
    assertThat(result.getAttribute(StageExecutionResult.COMMAND)).endsWith("echo test");
    assertThat(result.getAttribute(StageExecutionResult.EXIT_CODE)).isEqualTo("0");
    assertThat(result.getStdout()).contains("is submitted to default queue");

    while (true) {
      result = executor.execute(stage);
      if (!result.isActive()) {
        break;
      }

      try {
        Thread.sleep(Duration.ofSeconds(5).toMillis());
      } catch (InterruptedException ex) {
        Thread.currentThread().interrupt();
        break;
      }
    }

    assertThat(result.getResultType()).isEqualTo(StageExecutionResultType.SUCCESS);
    assertThat(result.getAttribute(StageExecutionResult.COMMAND)).isBlank();
    assertThat(result.getAttribute(StageExecutionResult.EXIT_CODE)).isEqualTo("0");
    // TODO: LSF may not immediately flush stdout
    // assertThat(result.getStdout()).contains("test\n");
  }
}