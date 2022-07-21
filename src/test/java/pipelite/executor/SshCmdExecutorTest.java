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
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import pipelite.PipeliteIdCreator;
import pipelite.PipeliteTestConfigWithServices;
import pipelite.configuration.properties.SshTestConfiguration;
import pipelite.stage.Stage;
import pipelite.stage.executor.StageExecutor;
import pipelite.stage.executor.StageExecutorResult;
import pipelite.stage.executor.StageExecutorResultAttribute;
import pipelite.stage.parameters.CmdExecutorParameters;

@SpringBootTest(
    classes = PipeliteTestConfigWithServices.class,
    properties = {"pipelite.service.force=true", "pipelite.service.name=SshCmdExecutorTest"})
@ActiveProfiles("test")
public class SshCmdExecutorTest {

  @Autowired SshTestConfiguration sshTestConfiguration;

  private static final String PIPELINE_NAME = PipeliteIdCreator.pipelineName();
  private static final String PROCESS_ID = PipeliteIdCreator.processId();

  @Test
  @EnabledIfEnvironmentVariable(named = "PIPELITE_TEST_SSH_HOST", matches = ".+")
  public void test() {
    String stageName = PipeliteIdCreator.stageName();

    CmdExecutor<CmdExecutorParameters> executor = StageExecutor.createCmdExecutor("echo test");
    executor.setExecutorParams(
        CmdExecutorParameters.builder()
            .host(sshTestConfiguration.getHost())
            .user(sshTestConfiguration.getUser())
            .timeout(Duration.ofSeconds(30))
            .build());
    Stage stage = Stage.builder().stageName(stageName).executor(executor).build();

    StageExecutorResult result = stage.execute();

    // Ignore timeout errors.
    if (result.isTimeoutError()) {
      return;
    }
    assertThat(result.isSuccess()).isTrue();
    assertThat(result.attribute(StageExecutorResultAttribute.COMMAND)).isEqualTo("echo test");
    assertThat(result.attribute(StageExecutorResultAttribute.EXIT_CODE)).isEqualTo("0");
    assertThat(result.stageLog()).contains("test\n");
  }
}
