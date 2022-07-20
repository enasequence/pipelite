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
import pipelite.service.PipeliteServices;
import pipelite.stage.executor.StageExecutor;
import pipelite.stage.executor.StageExecutorResultAttribute;
import pipelite.stage.parameters.SimpleLsfExecutorParameters;
import pipelite.stage.parameters.cmd.LogFileSavePolicy;

@SpringBootTest(
    classes = PipeliteTestConfigWithServices.class,
    properties = {"pipelite.service.force=true", "pipelite.service.name=SshSimpleLsfExecutorTest"})
@ActiveProfiles("test")
public class SshSimpleLsfExecutorTest {

  @Autowired LsfTestConfiguration lsfTestConfiguration;
  @Autowired PipeliteServices pipeliteServices;

  @Test
  @EnabledIfEnvironmentVariable(named = "PIPELITE_TEST_LSF_HOST", matches = ".+")
  public void testExecuteSuccess() {
    SimpleLsfExecutor executor = StageExecutor.createSimpleLsfExecutor("echo test");
    executor.setExecutorParams(
        SimpleLsfExecutorParameters.builder()
            .host(lsfTestConfiguration.getHost())
            .user(lsfTestConfiguration.getUser())
            .logDir(lsfTestConfiguration.getLogDir())
            .queue(lsfTestConfiguration.getQueue())
            .timeout(Duration.ofSeconds(30))
            .logSave(LogFileSavePolicy.ALWAYS)
            .logTimeout(Duration.ofSeconds(60))
            .build());

    AsyncExecutorTestHelper.testExecute(
        executor,
        pipeliteServices,
        result -> {
          assertThat(result.getAttribute(StageExecutorResultAttribute.COMMAND)).startsWith("bsub");
          assertThat(result.getAttribute(StageExecutorResultAttribute.COMMAND))
              .endsWith("echo test");
          assertThat(result.getAttribute(StageExecutorResultAttribute.EXIT_CODE)).isEqualTo("0");
          assertThat(result.getStageLog()).contains("is submitted to");
        },
        result -> {
          assertThat(result.isSuccess()).isTrue();
          assertThat(result.getAttribute(StageExecutorResultAttribute.EXIT_CODE)).isEqualTo("0");
          assertThat(result.getStageLog()).contains("test\n");
        });
  }

  @Test
  @EnabledIfEnvironmentVariable(named = "PIPELITE_TEST_LSF_HOST", matches = ".+")
  public void testExecuteError() {
    SimpleLsfExecutor executor = StageExecutor.createSimpleLsfExecutor("exit 5");
    executor.setExecutorParams(
        SimpleLsfExecutorParameters.builder()
            .host(lsfTestConfiguration.getHost())
            .user(lsfTestConfiguration.getUser())
            .logDir(lsfTestConfiguration.getLogDir())
            .queue(lsfTestConfiguration.getQueue())
            .timeout(Duration.ofSeconds(30))
            .logSave(LogFileSavePolicy.ALWAYS)
            .logTimeout(Duration.ofSeconds(60))
            .build());

    AsyncExecutorTestHelper.testExecute(
        executor,
        pipeliteServices,
        result -> {
          assertThat(result.getAttribute(StageExecutorResultAttribute.COMMAND)).startsWith("bsub");
          assertThat(result.getAttribute(StageExecutorResultAttribute.COMMAND)).endsWith("exit 5");
        },
        result -> {
          assertThat(result.isSuccess()).isFalse();
          assertThat(result.getAttribute(StageExecutorResultAttribute.EXIT_CODE)).isEqualTo("5");
        });
  }
}
