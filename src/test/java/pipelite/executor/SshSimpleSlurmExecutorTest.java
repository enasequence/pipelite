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
import pipelite.configuration.properties.SlurmTestConfiguration;
import pipelite.service.PipeliteServices;
import pipelite.stage.executor.StageExecutor;
import pipelite.stage.executor.StageExecutorResultAttribute;
import pipelite.stage.parameters.SimpleSlurmExecutorParameters;
import pipelite.stage.parameters.cmd.LogFileSavePolicy;
import pipelite.test.configuration.PipeliteTestConfigWithServices;

@SpringBootTest(
    classes = PipeliteTestConfigWithServices.class,
    properties = {
      "pipelite.service.force=true",
      "pipelite.service.name=SshSimpleSlurmExecutorTest"
    })
public class SshSimpleSlurmExecutorTest {

  @Autowired SlurmTestConfiguration slurmTestConfiguration;
  @Autowired PipeliteServices pipeliteServices;

  @Test
  public void testExecuteSuccess() {
    SimpleSlurmExecutor executor = StageExecutor.createSimpleSlurmExecutor("echo test");
    executor.setExecutorParams(
        SimpleSlurmExecutorParameters.builder()
            .host(slurmTestConfiguration.getHost())
            .user(slurmTestConfiguration.getUser())
            .logDir(slurmTestConfiguration.getLogDir())
            .queue(slurmTestConfiguration.getQueue())
            .memory(1)
            .cpu(1)
            .timeout(Duration.ofSeconds(30))
            .logSave(LogFileSavePolicy.ALWAYS)
            .logTimeout(Duration.ofSeconds(60))
            .build());

    AsyncExecutorTestHelper.testExecute(
        executor,
        pipeliteServices,
        result -> {
          assertThat(result.attribute(StageExecutorResultAttribute.COMMAND))
              .startsWith("sbatch << EOF");

          assertThat(result.attribute(StageExecutorResultAttribute.COMMAND))
              .contains("#SBATCH --job-name=");
          assertThat(result.attribute(StageExecutorResultAttribute.COMMAND))
              .contains("#SBATCH --output=");
          assertThat(result.attribute(StageExecutorResultAttribute.COMMAND))
              .contains("#SBATCH -n 1");
          assertThat(result.attribute(StageExecutorResultAttribute.COMMAND))
              .contains("#SBATCH --mem=\"1\"");
          assertThat(result.attribute(StageExecutorResultAttribute.COMMAND))
              .contains("#SBATCH -t 1");
          assertThat(result.attribute(StageExecutorResultAttribute.COMMAND))
              .contains("#SBATCH -p standard");

          assertThat(result.attribute(StageExecutorResultAttribute.COMMAND)).contains("mkdir -p");
          assertThat(result.attribute(StageExecutorResultAttribute.COMMAND))
              .contains("echo test >");
          assertThat(result.attribute(StageExecutorResultAttribute.COMMAND)).endsWith("2>&1\nEOF");
          assertThat(result.attribute(StageExecutorResultAttribute.EXIT_CODE)).isEqualTo("0");
          assertThat(result.stdOut()).contains("Submitted batch job");
          assertThat(result.stageLog()).contains("Submitted batch job");
        },
        result -> {
          assertThat(result.isSuccess()).isTrue();
          assertThat(result.attribute(StageExecutorResultAttribute.EXIT_CODE)).isEqualTo("0");
          assertThat(result.stdOut()).contains("test\n");
          assertThat(result.stageLog()).contains("test\n");
        });
  }

  @Test
  public void testExecuteError() {
    SimpleSlurmExecutor executor = StageExecutor.createSimpleSlurmExecutor("exit 5");
    executor.setExecutorParams(
        SimpleSlurmExecutorParameters.builder()
            .host(slurmTestConfiguration.getHost())
            .user(slurmTestConfiguration.getUser())
            .logDir(slurmTestConfiguration.getLogDir())
            .queue(slurmTestConfiguration.getQueue())
            .memory(1)
            .cpu(1)
            .timeout(Duration.ofSeconds(30))
            .logSave(LogFileSavePolicy.ALWAYS)
            .logTimeout(Duration.ofSeconds(60))
            .build());

    AsyncExecutorTestHelper.testExecute(
        executor,
        pipeliteServices,
        result -> {
          assertThat(result.attribute(StageExecutorResultAttribute.COMMAND))
              .startsWith("sbatch << EOF");

          assertThat(result.attribute(StageExecutorResultAttribute.COMMAND))
              .contains("#SBATCH --job-name=");
          assertThat(result.attribute(StageExecutorResultAttribute.COMMAND))
              .contains("#SBATCH --output=");
          assertThat(result.attribute(StageExecutorResultAttribute.COMMAND))
              .contains("#SBATCH -n 1");
          assertThat(result.attribute(StageExecutorResultAttribute.COMMAND))
              .contains("#SBATCH --mem=\"1\"");
          assertThat(result.attribute(StageExecutorResultAttribute.COMMAND))
              .contains("#SBATCH -t 1");
          assertThat(result.attribute(StageExecutorResultAttribute.COMMAND))
              .contains("#SBATCH -p standard");

          assertThat(result.attribute(StageExecutorResultAttribute.COMMAND)).contains("mkdir -p");
          assertThat(result.attribute(StageExecutorResultAttribute.COMMAND)).contains("exit 5 >");
          assertThat(result.attribute(StageExecutorResultAttribute.COMMAND)).endsWith("2>&1\nEOF");
          assertThat(result.attribute(StageExecutorResultAttribute.EXIT_CODE)).isEqualTo("0");
          assertThat(result.stdOut()).contains("Submitted batch job");
          assertThat(result.stageLog()).contains("Submitted batch job");
        },
        result -> {
          assertThat(result.isSuccess()).isFalse();
          assertThat(result.attribute(StageExecutorResultAttribute.EXIT_CODE)).isEqualTo("5");
        });
  }
}
