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
import pipelite.PipeliteTestConfigWithServices;
import pipelite.UniqueStringGenerator;
import pipelite.configuration.properties.SshTestConfiguration;
import pipelite.stage.Stage;
import pipelite.stage.StageState;
import pipelite.stage.executor.ErrorType;
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

  private static final String PIPELINE_NAME =
      UniqueStringGenerator.randomPipelineName(SshCmdExecutorTest.class);
  private static final String PROCESS_ID =
      UniqueStringGenerator.randomProcessId(SshCmdExecutorTest.class);

  @Test
  public void test() {
    String stageName = UniqueStringGenerator.randomStageName(this.getClass());

    CmdExecutor<CmdExecutorParameters> executor = StageExecutor.createCmdExecutor("echo test");
    executor.setExecutorParams(
        CmdExecutorParameters.builder()
            .host(sshTestConfiguration.getHost())
            .timeout(Duration.ofSeconds(30))
            .build());
    Stage stage = Stage.builder().stageName(stageName).executor(executor).build();

    StageExecutorResult result = stage.execute(PIPELINE_NAME, PROCESS_ID);
    // Ignore timeout errors.
    if (result.isErrorType(ErrorType.TIMEOUT_ERROR)) {
      return;
    }
    assertThat(result.getStageState()).isEqualTo(StageState.SUCCESS);
    assertThat(result.getAttribute(StageExecutorResultAttribute.COMMAND)).isEqualTo("echo test");
    assertThat(result.getAttribute(StageExecutorResultAttribute.EXIT_CODE)).isEqualTo("0");
    assertThat(result.getStageLog()).contains("test\n");
  }

  /*
  @Test
  public void testSingularity() {

    CmdExecutor<CmdExecutorParameters> executor = StageExecutor.createSshCmdExecutor("");
    executor.setExecutorParams(
        CmdExecutorParameters.builder()
            .host(singularityTestConfiguration.getHost())
            .singularityImage("docker://enasequence/webin-cli")
            .timeout(Duration.ofSeconds(30))
            .build());
    String stageName = "testStageName";
    Stage stage = Stage.builder().stageName(stageName).executor(executor).build();

    StageExecutorResult result = stage.execute(PIPELINE_NAME, PROCESS_ID);
    // Ignore timeout errors.
    if (InternalError.TIMEOUT == result.getInternalError()) {
      return;
    }
    assertThat(result.getStageState()).isEqualTo(StageExecutorstageState.ERROR);
    assertThat(result.getAttribute(StageExecutorResultAttribute.COMMAND))
        .isEqualTo("singularity run docker://enasequence/webin-cli ");
    assertThat(result.getStdout()).contains("Creating container runtime...");
    assertThat(result.getAttribute(StageExecutorResultAttribute.EXIT_CODE)).isEqualTo("2");
  }
   */
}
