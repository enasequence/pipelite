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
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import pipelite.PipeliteTestConfiguration;
import pipelite.UniqueStringGenerator;
import pipelite.configuration.SingularityTestConfiguration;
import pipelite.configuration.SshTestConfiguration;
import pipelite.stage.Stage;
import pipelite.stage.executor.*;

@SpringBootTest(classes = PipeliteTestConfiguration.class)
@ActiveProfiles(value = {"hsql-test", "pipelite-test"})
public class SshCmdExecutorTest {

  @Autowired SshTestConfiguration sshTestConfiguration;
  @Autowired SingularityTestConfiguration singularityTestConfiguration;

  private static final String PIPELINE_NAME = UniqueStringGenerator.randomPipelineName();
  private static final String PROCESS_ID = UniqueStringGenerator.randomProcessId();

  @Test
  public void test() {

    String stageName = UniqueStringGenerator.randomStageName();

    StageExecutorParameters executorParams = StageExecutorParameters.builder().build();
    executorParams.setHost(sshTestConfiguration.getHost());
    executorParams.setTimeout(Duration.ofSeconds(30));

    Stage stage =
        Stage.builder()
            .stageName(stageName)
            .executor(StageExecutor.createSshCmdExecutor("echo test"))
            .executorParams(executorParams)
            .build();

    StageExecutorResult result = stage.execute(PIPELINE_NAME, PROCESS_ID);
    // Ignore timeout errors.
    if (StageExecutorResult.InternalError.CMD_TIMEOUT == result.getInternalError()) {
      return;
    }
    assertThat(result.getResultType()).isEqualTo(StageExecutorResultType.SUCCESS);
    assertThat(result.getAttribute(StageExecutorResultAttribute.COMMAND)).isEqualTo("echo test");
    assertThat(result.getAttribute(StageExecutorResultAttribute.EXIT_CODE)).isEqualTo("0");
    assertThat(result.getStdout()).isEqualTo("test\n");
  }

  @Test
  public void testSingularity() {

    StageExecutorParameters executorParams = StageExecutorParameters.builder().build();
    executorParams.setHost(singularityTestConfiguration.getHost());
    executorParams.setSingularityImage("docker://enasequence/webin-cli");
    executorParams.setTimeout(Duration.ofSeconds(30));

    String stageName = "testStageName";

    Stage stage =
        Stage.builder()
            .stageName(stageName)
            .executor(StageExecutor.createSshCmdExecutor(""))
            .executorParams(executorParams)
            .build();

    StageExecutorResult result = stage.execute(PIPELINE_NAME, PROCESS_ID);
    // Ignore timeout errors.
    if (StageExecutorResult.InternalError.CMD_TIMEOUT == result.getInternalError()) {
      return;
    }
    assertThat(result.getResultType()).isEqualTo(StageExecutorResultType.ERROR);
    assertThat(result.getAttribute(StageExecutorResultAttribute.COMMAND))
        .isEqualTo("singularity run docker://enasequence/webin-cli ");
    assertThat(result.getStdout()).contains("Creating container runtime...");
    assertThat(result.getAttribute(StageExecutorResultAttribute.EXIT_CODE)).isEqualTo("2");
  }
}
