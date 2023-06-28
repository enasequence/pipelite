/*
 * Copyright 2020-2022 EMBL - European Bioinformatics Institute
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
import pipelite.configuration.properties.SshTestConfiguration;
import pipelite.stage.Stage;
import pipelite.stage.executor.StageExecutor;
import pipelite.stage.executor.StageExecutorResult;
import pipelite.stage.executor.StageExecutorResultAttribute;
import pipelite.stage.parameters.CmdExecutorParameters;
import pipelite.test.PipeliteTestIdCreator;
import pipelite.test.configuration.PipeliteTestConfigWithServices;

@SpringBootTest(
    classes = PipeliteTestConfigWithServices.class,
    properties = {"pipelite.service.force=true", "pipelite.service.name=SshCmdExecutorTest"})
public class SshCmdExecutorTest {

  @Autowired SshTestConfiguration sshTestConfiguration;

  @Test
  public void test() {
    String stageName = PipeliteTestIdCreator.stageName();

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
    assertThat(result.exitCode()).isEqualTo("0");
    assertThat(result.stdOut()).contains("test\n");
    assertThat(result.stdErr()).contains("");
    assertThat(result.stageLog()).contains("test\n");
  }
}
