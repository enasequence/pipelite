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
package pipelite.executor;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import pipelite.PipeliteTestConfiguration;
import pipelite.UniqueStringGenerator;
import pipelite.configuration.LsfTestConfiguration;
import pipelite.exception.PipeliteInterruptedException;
import pipelite.stage.Stage;
import pipelite.stage.executor.*;
import pipelite.stage.parameters.LsfExecutorParameters;
import pipelite.time.Time;

@SpringBootTest(classes = PipeliteTestConfiguration.class)
@ActiveProfiles(value = {"hsql-test", "pipelite-test"})
public class LsfSshCmdExecutorTest {

  @Autowired LsfTestConfiguration lsfTestConfiguration;

  private final String PIPELINE_NAME = UniqueStringGenerator.randomPipelineName();
  private final String PROCESS_ID = UniqueStringGenerator.randomProcessId();

  @Test
  public void test() {

    LsfExecutor executor = StageExecutor.createSshLsfExecutor("echo test");
    executor.setExecutorParams(
        LsfExecutorParameters.builder()
            .host(lsfTestConfiguration.getHost())
            .workDir(lsfTestConfiguration.getWorkDir())
            .timeout(Duration.ofSeconds(60))
            .build());

    String stageName = UniqueStringGenerator.randomStageName();

    Stage stage = Stage.builder().stageName(stageName).executor(executor).build();

    StageExecutorResult result = executor.execute(PIPELINE_NAME, PROCESS_ID, stage);
    assertThat(result.getResultType()).isEqualTo(StageExecutorResultType.ACTIVE);
    assertThat(result.getAttribute(StageExecutorResultAttribute.COMMAND)).startsWith("bsub");
    assertThat(result.getAttribute(StageExecutorResultAttribute.COMMAND)).endsWith("echo test");
    assertThat(result.getAttribute(StageExecutorResultAttribute.EXIT_CODE)).isEqualTo("0");
    assertThat(result.getStdout()).contains("is submitted to default queue");

    while (true) {
      result = executor.execute(PIPELINE_NAME, PROCESS_ID, stage);
      if (!result.isActive()) {
        break;
      }
      if (!Time.wait(5, TimeUnit.SECONDS)) {
        throw new PipeliteInterruptedException("Stage launcher was interrupted");
      }
    }

    // Ignore timeout errors.
    if (StageExecutorResult.InternalError.CMD_TIMEOUT == result.getInternalError()) {
      return;
    }

    assertThat(result.getResultType()).isEqualTo(StageExecutorResultType.SUCCESS);
    assertThat(result.getAttribute(StageExecutorResultAttribute.COMMAND)).isBlank();
    assertThat(result.getAttribute(StageExecutorResultAttribute.EXIT_CODE)).isEqualTo("0");
    assertThat(result.getStdout()).contains("test\n");
  }
}
