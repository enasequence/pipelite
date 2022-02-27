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
import pipelite.PipeliteTestConfigWithServices;
import pipelite.configuration.properties.LsfTestConfiguration;
import pipelite.service.StageService;
import pipelite.stage.executor.StageExecutor;
import pipelite.stage.executor.StageExecutorResultAttribute;
import pipelite.stage.parameters.LsfExecutorParameters;
import pipelite.stage.parameters.cmd.LogFileSavePolicy;

@SpringBootTest(
    classes = PipeliteTestConfigWithServices.class,
    properties = {"pipelite.service.force=true", "pipelite.service.name=SshLsfExecutorTest"})
@ActiveProfiles("test")
public class SshLsfExecutorTest {

  @Autowired LsfTestConfiguration lsfTestConfiguration;
  @Autowired StageService stageService;

  @Test
  @EnabledIfEnvironmentVariable(named = "PIPELITE_TEST_LSF_HOST", matches = ".+")
  public void testExecuteSuccess() {
    LsfExecutor executor = StageExecutor.createLsfExecutor("");
    executor.setExecutorParams(
        LsfExecutorParameters.builder()
            .host(lsfTestConfiguration.getHost())
            .user(lsfTestConfiguration.getUser())
            .logDir(lsfTestConfiguration.getLogDir())
            .definitionDir(lsfTestConfiguration.getDefinitionDir())
            .timeout(Duration.ofSeconds(60))
            .logSave(LogFileSavePolicy.ALWAYS)
            .definition("pipelite/executor/lsf.yaml")
            .format(LsfExecutorParameters.Format.YAML)
            .build());

    AsyncExecutorTestHelper.testExecute(
        executor,
        stageService,
        result -> {
          assertThat(result.getAttribute(StageExecutorResultAttribute.COMMAND)).startsWith("bsub");
          assertThat(result.getAttribute(StageExecutorResultAttribute.COMMAND)).contains("-yaml");
          assertThat(result.getAttribute(StageExecutorResultAttribute.EXIT_CODE)).isEqualTo("0");
          assertThat(result.getStageLog()).contains("is submitted to default queue");
        },
        result -> {
          assertThat(result.isSuccess()).isTrue();
          assertThat(result.getAttribute(StageExecutorResultAttribute.EXIT_CODE)).isEqualTo("0");
          assertThat(result.getStageLog()).contains("test\n");
        });
  }
}
