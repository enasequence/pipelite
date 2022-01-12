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
package pipelite.helper.process;

import java.time.Duration;
import pipelite.configuration.properties.KubernetesTestConfiguration;
import pipelite.helper.TestType;
import pipelite.helper.entity.StageEntityTestHelper;
import pipelite.process.builder.ProcessBuilder;
import pipelite.stage.parameters.KubernetesExecutorParameters;

public class SingleStageKubernetesTestProcessConfiguration
    extends SingleStageTestProcessConfiguration<SingleStageKubernetesTestProcessConfiguration> {

  private final KubernetesTestConfiguration kubernetesTestConfiguration;

  public SingleStageKubernetesTestProcessConfiguration(
      TestType testType,
      int immediateRetries,
      int maximumRetries,
      KubernetesTestConfiguration kubernetesTestConfiguration) {
    super(
        testType,
        immediateRetries,
        maximumRetries,
        (stageService, pipelineName, processId, stageName) ->
            StageEntityTestHelper.assertSubmittedKubernetesExecutorStageEntity(
                testType,
                stageService,
                kubernetesTestConfiguration,
                pipelineName,
                processId,
                stageName,
                immediateRetries,
                maximumRetries),
        (stageService, pipelineName, processId, stageName) ->
            StageEntityTestHelper.assertCompletedKubernetesExecutorStageEntity(
                testType,
                stageService,
                kubernetesTestConfiguration,
                pipelineName,
                processId,
                stageName,
                immediateRetries,
                maximumRetries));
    this.kubernetesTestConfiguration = kubernetesTestConfiguration;
  }

  @Override
  protected void testConfigureProcess(ProcessBuilder builder) {
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
