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
package pipelite.tester.process;

import java.time.Duration;
import pipelite.configuration.properties.KubernetesTestConfiguration;
import pipelite.process.builder.ProcessBuilder;
import pipelite.stage.parameters.KubernetesExecutorParameters;
import pipelite.tester.TestTypeConfiguration;
import pipelite.tester.entity.StageEntityAsserter;

public class SingleStageKubernetesTestProcessConfiguration
    extends SingleStageTestProcessConfiguration {

  private final KubernetesTestConfiguration kubernetesTestConfiguration;

  public SingleStageKubernetesTestProcessConfiguration(
      TestTypeConfiguration testConfiguration,
      KubernetesTestConfiguration kubernetesTestConfiguration) {
    super(
        testConfiguration,
        (stageService, pipelineName, processId, stageName) ->
            StageEntityAsserter.assertSubmittedKubernetesStageEntity(
                stageService,
                testConfiguration,
                kubernetesTestConfiguration,
                pipelineName,
                processId,
                stageName),
        (stageService, pipelineName, processId, stageName) ->
            StageEntityAsserter.assertCompletedKubernetesStageEntity(
                stageService,
                testConfiguration,
                kubernetesTestConfiguration,
                pipelineName,
                processId,
                stageName));
    this.kubernetesTestConfiguration = kubernetesTestConfiguration;
  }

  @Override
  protected void configure(ProcessBuilder builder) {
    KubernetesExecutorParameters.KubernetesExecutorParametersBuilder<?, ?> executorParamsBuilder =
        KubernetesExecutorParameters.builder();
    executorParamsBuilder
        .namespace(kubernetesTestConfiguration.getNamespace())
        .timeout(Duration.ofSeconds(180))
        .maximumRetries(maximumRetries())
        .immediateRetries(immediateRetries());
    testExecutorParams(executorParamsBuilder);
    KubernetesExecutorParameters executorParams = executorParamsBuilder.build();
    executorParams.setPermanentErrors(testType().permanentErrors());
    builder
        .execute(stageName())
        .withKubernetesExecutor(testType().image(), testType().imageArgs(), executorParams);
  }

  protected void testExecutorParams(
      KubernetesExecutorParameters.KubernetesExecutorParametersBuilder<?, ?>
          executorParamsBuilder) {}
}
