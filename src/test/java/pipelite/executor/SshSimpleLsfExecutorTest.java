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
import pipelite.configuration.properties.LsfTestConfiguration;
import pipelite.service.StageService;
import pipelite.stage.Stage;
import pipelite.stage.StageState;
import pipelite.stage.executor.StageExecutor;
import pipelite.stage.executor.StageExecutorRequest;
import pipelite.stage.executor.StageExecutorResult;
import pipelite.stage.executor.StageExecutorResultAttribute;
import pipelite.stage.parameters.SimpleLsfExecutorParameters;
import pipelite.time.Time;

@SpringBootTest(
    classes = PipeliteTestConfigWithServices.class,
    properties = {"pipelite.service.force=true", "pipelite.service.name=SshSimpleLsfExecutorTest"})
@ActiveProfiles("test")
public class SshSimpleLsfExecutorTest {

  @Autowired LsfTestConfiguration lsfTestConfiguration;
  @Autowired StageService stageService;

  private final String PIPELINE_NAME =
      UniqueStringGenerator.randomPipelineName(SshSimpleLsfExecutorTest.class);
  private final String PROCESS_ID =
      UniqueStringGenerator.randomProcessId(SshSimpleLsfExecutorTest.class);

  @Test
  public void test() {
    SimpleLsfExecutor executor = StageExecutor.createSimpleLsfExecutor("echo test");
    executor.setExecutorParams(
        SimpleLsfExecutorParameters.builder()
            .host(lsfTestConfiguration.getHost())
            .workDir(lsfTestConfiguration.getWorkDir())
            .timeout(Duration.ofSeconds(180))
            .build());

    String stageName = UniqueStringGenerator.randomStageName(this.getClass());
    Stage stage = Stage.builder().stageName(stageName).executor(executor).build();
    StageExecutorRequest request =
        StageExecutorRequest.builder()
            .pipelineName(PIPELINE_NAME)
            .processId(PROCESS_ID)
            .stage(stage)
            .build();

    executor.prepareExecute(stageService.getExecutorContextCache());
    StageExecutorResult result = executor.execute(request);
    assertThat(result.getStageState()).isEqualTo(StageState.ACTIVE);
    assertThat(result.getAttribute(StageExecutorResultAttribute.COMMAND)).startsWith("bsub");
    assertThat(result.getAttribute(StageExecutorResultAttribute.COMMAND)).endsWith("echo test");
    assertThat(result.getAttribute(StageExecutorResultAttribute.EXIT_CODE)).isEqualTo("0");
    assertThat(result.getStageLog()).contains("is submitted to default queue");

    while (true) {
      result = executor.execute(request);
      if (!result.isActive()) {
        break;
      }
      Time.wait(Duration.ofSeconds(5));
    }

    // Ignore timeout errors.
    if (result.isTimeoutError()) {
      return;
    }

    assertThat(result.getStageState()).isEqualTo(StageState.SUCCESS);
    assertThat(result.getAttribute(StageExecutorResultAttribute.EXIT_CODE)).isEqualTo("0");
    assertThat(result.getStageLog()).contains("test\n");
  }
}
