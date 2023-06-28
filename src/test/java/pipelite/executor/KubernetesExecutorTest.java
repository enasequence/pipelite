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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import pipelite.configuration.properties.KubernetesTestConfiguration;
import pipelite.service.PipeliteServices;
import pipelite.stage.executor.StageExecutor;
import pipelite.stage.parameters.KubernetesExecutorParameters;
import pipelite.stage.parameters.cmd.LogFileSavePolicy;
import pipelite.test.configuration.PipeliteTestConfigWithServices;

@SpringBootTest(
    classes = PipeliteTestConfigWithServices.class,
    properties = {"pipelite.service.force=true", "pipelite.service.name=KubernetesExecutorTest"})
public class KubernetesExecutorTest {

  @Autowired PipeliteServices pipeliteServices;
  @Autowired KubernetesTestConfiguration testConfiguration;

  @Test
  public void kubernetesJobId() {
    String jobId = KubernetesExecutor.kubernetesJobName();
    assertThat(jobId)
        .matches(
            "^pipelite-\\b[0-9a-f]{8}\\b-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-\\b[0-9a-f]{12}\\b$");
    assertThat(jobId).matches("[a-z0-9]([\\-a-z0-9]*[a-z0-9])?");
    assertThat(jobId.length()).isLessThan(64);
  }

  @Test
  public void testExecuteSuccess() {
    String image = "hello-world:linux";
    List<String> imageArgs = Collections.emptyList();
    KubernetesExecutor executor = StageExecutor.createKubernetesExecutor(image, imageArgs);

    executor.setExecutorParams(
        KubernetesExecutorParameters.builder()
            .namespace(testConfiguration.getNamespace())
            .timeout(Duration.ofSeconds(30))
            .logSave(LogFileSavePolicy.ALWAYS)
            .build());

    AsyncExecutorTestHelper.testExecute(
        executor,
        pipeliteServices,
        result -> {},
        result -> {
          assertThat(result.isSuccess()).isTrue();
          assertThat(result.exitCode()).isEqualTo("0");
          assertThat(result.stageLog())
              .startsWith(
                  "\n"
                      + "Hello from Docker!\n"
                      + "This message shows that your installation appears to be working correctly.");
        });
  }

  @Test
  public void testExecuteError() {
    String image = "debian:10.11";
    List<String> imageArgs = Arrays.asList("bash", "-c", "exit 5");
    KubernetesExecutor executor = StageExecutor.createKubernetesExecutor(image, imageArgs);

    executor.setExecutorParams(
        KubernetesExecutorParameters.builder()
            .namespace(testConfiguration.getNamespace())
            .timeout(Duration.ofSeconds(30))
            .logSave(LogFileSavePolicy.ALWAYS)
            .build());

    AsyncExecutorTestHelper.testExecute(
        executor,
        pipeliteServices,
        result -> {},
        result -> {
          assertThat(result.isSuccess()).isFalse();
          assertThat(result.exitCode()).isEqualTo("5");
        });
  }
}
