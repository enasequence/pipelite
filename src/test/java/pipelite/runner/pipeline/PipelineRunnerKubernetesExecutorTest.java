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
package pipelite.runner.pipeline;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Profile;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import pipelite.PipeliteTestConfigWithManager;
import pipelite.configuration.properties.KubernetesTestConfiguration;
import pipelite.manager.ProcessRunnerPoolManager;
import pipelite.metrics.PipeliteMetrics;
import pipelite.service.ProcessService;
import pipelite.service.RunnerService;
import pipelite.service.StageService;
import pipelite.stage.parameters.KubernetesExecutorParameters;
import pipelite.tester.TestType;
import pipelite.tester.TestTypeConfiguration;
import pipelite.tester.pipeline.ConfigurableTestPipeline;
import pipelite.tester.process.SingleStageKubernetesTestProcessConfiguration;
import pipelite.tester.process.SingleStageTestProcessConfiguration;

@SpringBootTest(
    classes = PipeliteTestConfigWithManager.class,
    properties = {
      "pipelite.service.force=true",
      "pipelite.service.name=PipelineRunnerTest",
      "pipelite.advanced.processRunnerFrequency=250ms",
      "pipelite.advanced.shutdownIfIdle=true"
    })
@ActiveProfiles({"test", "PipelineRunnerKubernetesExecutorTest"})
@DirtiesContext
public class PipelineRunnerKubernetesExecutorTest {

  private static final int PROCESS_CNT = 2;
  private static final int IMMEDIATE_RETRIES = 3;
  private static final int MAXIMUM_RETRIES = 3;
  private static final int PARALLELISM = 1;

  @Autowired private ProcessRunnerPoolManager processRunnerPoolManager;
  @Autowired private ProcessService processService;
  @Autowired private StageService stageService;
  @Autowired private RunnerService runnerService;
  @Autowired private PipeliteMetrics metrics;

  @Autowired
  private List<ConfigurableTestPipeline<SingleStageTestProcessConfiguration>> testPipelines;

  private static TestTypeConfiguration testTypeConfiguration(TestType testType) {
    return new TestTypeConfiguration(testType, IMMEDIATE_RETRIES, MAXIMUM_RETRIES);
  }

  @Profile("PipelineRunnerKubernetesExecutorTest")
  @TestConfiguration
  static class TestConfig {
    @Autowired private KubernetesTestConfiguration kubernetesTestConfiguration;

    @Bean
    public KubernetesPipeline KubernetesSuccessPipeline() {
      return new KubernetesPipeline(TestType.SUCCESS, kubernetesTestConfiguration);
    }

    @Bean
    public KubernetesPipeline KubernetesNonPermanentErrorPipeline() {
      return new KubernetesPipeline(TestType.NON_PERMANENT_ERROR, kubernetesTestConfiguration);
    }

    @Bean
    public KubernetesPermanentErrorPipeline KubernetesPermanentErrorPipeline() {
      return new KubernetesPermanentErrorPipeline(kubernetesTestConfiguration);
    }
  }

  private static class KubernetesPipeline extends ConfigurableTestPipeline {
    public KubernetesPipeline(
        TestType testType, KubernetesTestConfiguration kubernetesTestConfiguration) {
      super(
          PARALLELISM,
          PROCESS_CNT,
          new SingleStageKubernetesTestProcessConfiguration(
              testTypeConfiguration(testType), kubernetesTestConfiguration));
    }
  }

  private static class KubernetesPermanentErrorPipeline extends ConfigurableTestPipeline {
    public KubernetesPermanentErrorPipeline(
        KubernetesTestConfiguration kubernetesTestConfiguration) {
      super(
          PARALLELISM,
          PROCESS_CNT,
          new SingleStageKubernetesTestProcessConfiguration(
              testTypeConfiguration(TestType.PERMANENT_ERROR), kubernetesTestConfiguration) {
            @Override
            protected void testExecutorParams(
                KubernetesExecutorParameters.KubernetesExecutorParametersBuilder<?, ?>
                    executorParamsBuilder) {
              executorParamsBuilder.permanentError(0);
            }
          });
    }
  }

  private void assertPipeline(ConfigurableTestPipeline<SingleStageTestProcessConfiguration> f) {
    for (PipelineRunner pipelineRunner : runnerService.getPipelineRunners()) {
      assertThat(pipelineRunner.getActiveProcessRunners().size()).isEqualTo(0);
    }
    SingleStageTestProcessConfiguration testProcessConfiguration = f.getRegisteredPipeline();
    assertThat(testProcessConfiguration.configuredProcessIds().size()).isEqualTo(PROCESS_CNT);
    testProcessConfiguration.assertCompletedMetrics(metrics, PROCESS_CNT);
    testProcessConfiguration.assertCompletedProcessEntities(processService, PROCESS_CNT);
    testProcessConfiguration.assertCompletedStageEntities(stageService, PROCESS_CNT);
  }

  @Test
  @EnabledIfEnvironmentVariable(named = "PIPELITE_TEST_KUBERNETES_KUBECONFIG", matches = ".+")
  public void runPipelines() {
    processRunnerPoolManager.createPools();
    processRunnerPoolManager.startPools();
    processRunnerPoolManager.waitPoolsToStop();

    for (ConfigurableTestPipeline<SingleStageTestProcessConfiguration> f : testPipelines) {
      assertPipeline(f);
    }
  }
}
