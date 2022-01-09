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

import io.fabric8.kubernetes.api.model.batch.v1.JobStatus;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import pipelite.PipeliteTestConfigWithServices;
import pipelite.UniqueStringGenerator;
import pipelite.configuration.properties.KubernetesTestConfiguration;
import pipelite.service.StageService;
import pipelite.stage.Stage;
import pipelite.stage.executor.*;
import pipelite.stage.parameters.KubernetesExecutorParameters;
import pipelite.time.Time;

@SpringBootTest(
    classes = PipeliteTestConfigWithServices.class,
    properties = {"pipelite.service.force=true", "pipelite.service.name=KubernetesExecutorTest"})
@ActiveProfiles("test")
public class KubernetesExecutorTest {

  private final String PIPELINE_NAME =
      UniqueStringGenerator.randomPipelineName(KubernetesExecutorTest.class);
  private final String PROCESS_ID =
      UniqueStringGenerator.randomProcessId(KubernetesExecutorTest.class);

  @Autowired StageService stageService;
  @Autowired KubernetesTestConfiguration testConfiguration;

  @Test
  public void describeJobsStateActive() {
    JobStatus jobStatus = new JobStatus();
    assertThat(KubernetesExecutor.describeJobsResultFromStatus(jobStatus).isActive()).isTrue();
    jobStatus.setActive(1);
    assertThat(KubernetesExecutor.describeJobsResultFromStatus(jobStatus).isActive()).isTrue();
  }

  @Test
  public void describeJobsStateSuccess() {
    JobStatus jobStatus = new JobStatus();
    jobStatus.setSucceeded(1);
    assertThat(KubernetesExecutor.describeJobsResultFromStatus(jobStatus).isSuccess()).isTrue();
  }

  @Test
  public void describeJobsStateError() {
    JobStatus jobStatus = new JobStatus();
    jobStatus.setFailed(1);
    assertThat(KubernetesExecutor.describeJobsResultFromStatus(jobStatus).isError()).isTrue();
  }

  @Test
  public void kubernetesJobId() {
    String jobId = KubernetesExecutor.kubernetesJobId();
    assertThat(jobId)
        .matches(
            "^pipelite-\\b[0-9a-f]{8}\\b-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-\\b[0-9a-f]{12}\\b$");
    assertThat(jobId).matches("[a-z0-9]([\\-a-z0-9]*[a-z0-9])?");
    assertThat(jobId.length()).isLessThan(64);
  }

  @Test
  @EnabledIfEnvironmentVariable(named = "PIPELITE_TEST_KUBERNETES_KUBECONFIG", matches = ".+")
  public void testSuccess() {
    String image = "hello-world";
    List<String> imageArgs = Collections.emptyList();
    KubernetesExecutor executor = StageExecutor.createKubernetesExecutor(image, imageArgs);

    executor.setExecutorParams(
        KubernetesExecutorParameters.builder()
            .namespace(testConfiguration.getNamespace())
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

    executor.prepareExecute(stageService.getExecutorDescribeJobsCache());

    StageExecutorResult result = executor.execute(request);
    assertThat(result.isSubmitted()).isTrue();

    while (true) {
      result = executor.execute(request);
      if (!result.isActive()) {
        break;
      }
      Time.wait(Duration.ofSeconds(1));
    }

    // Ignore timeout errors.
    if (result.isErrorType(ErrorType.TIMEOUT_ERROR)) {
      return;
    }

    assertThat(result.isSuccess()).isTrue();
    assertThat(result.getAttribute(StageExecutorResultAttribute.EXIT_CODE)).isEqualTo("0");
    assertThat(result.getStageLog())
        .startsWith(
            "\n"
                + "Hello from Docker!\n"
                + "This message shows that your installation appears to be working correctly.");
  }

  @Test
  @EnabledIfEnvironmentVariable(named = "PIPELITE_TEST_KUBERNETES_KUBECONFIG", matches = ".+")
  public void testError() {
    String image = "debian";
    List<String> imageArgs = Arrays.asList("bash", "-c", "exit 5");
    KubernetesExecutor executor = StageExecutor.createKubernetesExecutor(image, imageArgs);

    executor.setExecutorParams(
        KubernetesExecutorParameters.builder()
            .namespace(testConfiguration.getNamespace())
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

    executor.prepareExecute(stageService.getExecutorDescribeJobsCache());

    StageExecutorResult result = executor.execute(request);
    assertThat(result.isSubmitted()).isTrue();

    while (true) {
      result = executor.execute(request);
      if (!result.isActive()) {
        break;
      }
      // Time.wait(Duration.ofSeconds(1));
      Time.wait(Duration.ofMillis(100));
    }

    // Ignore timeout errors.
    if (result.isErrorType(ErrorType.TIMEOUT_ERROR)) {
      return;
    }

    assertThat(result.isSuccess()).isFalse();
    assertThat(result.getAttribute(StageExecutorResultAttribute.EXIT_CODE)).isEqualTo("5");
  }
}
