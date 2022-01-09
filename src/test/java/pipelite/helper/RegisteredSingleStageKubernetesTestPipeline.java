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
package pipelite.helper;

import pipelite.configuration.properties.KubernetesTestConfiguration;
import pipelite.helper.entity.StageEntityTestHelper;
import pipelite.process.builder.ProcessBuilder;
import pipelite.stage.parameters.KubernetesExecutorParameters;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;

public class RegisteredSingleStageKubernetesTestPipeline
    extends RegisteredSingleStageTestPipeline<RegisteredSingleStageKubernetesTestPipeline> {

  private final String image = "debian";
  private final List<String> imageArgs;
  private final int exitCode;
  private final KubernetesTestConfiguration kubernetesTestConfiguration;
  private KubernetesExecutorParameters executorParams;

  public RegisteredSingleStageKubernetesTestPipeline(
      TestType testType,
      int exitCode,
      int immediateRetries,
      int maximumRetries,
      KubernetesTestConfiguration kubernetesTestConfiguration) {
    super(
        testType,
        immediateRetries,
        maximumRetries,
        (stageService, pipelineName, processId, stageName, thisPipeline) ->
            StageEntityTestHelper.assertSubmittedKubernetesExecutorStageEntity(
                stageService,
                kubernetesTestConfiguration,
                pipelineName,
                processId,
                stageName,
                thisPipeline.executorParams().getPermanentErrors(),
                thisPipeline.image,
                thisPipeline.imageArgs,
                thisPipeline.kubernetesTestConfiguration.getNamespace(),
                immediateRetries,
                maximumRetries),
        (stageService, pipelineName, processId, stageName, thisPipeline) ->
            StageEntityTestHelper.assertCompletedKubernetesExecutorStageEntity(
                testType,
                stageService,
                kubernetesTestConfiguration,
                pipelineName,
                processId,
                stageName,
                thisPipeline.executorParams().getPermanentErrors(),
                thisPipeline.image,
                thisPipeline.imageArgs,
                thisPipeline.kubernetesTestConfiguration.getNamespace(),
                thisPipeline.exitCode(),
                immediateRetries,
                maximumRetries));
    this.imageArgs = imageArgs(exitCode);
    this.exitCode = exitCode;
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
    executorParams = executorParamsBuilder.build();
    builder.execute(stageName()).withKubernetesExecutor(image, imageArgs, executorParams);
  }

  protected void testExecutorParams(
      KubernetesExecutorParameters.KubernetesExecutorParametersBuilder<?, ?>
          executorParamsBuilder) {}

  public static List<String> imageArgs(int exitCode) {
    return Arrays.asList("bash", "-c", "exit " + exitCode);
  }

  public int exitCode() {
    return exitCode;
  }

  public KubernetesExecutorParameters executorParams() {
    return executorParams;
  }
}
