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

import pipelite.configuration.properties.KubernetesTestConfiguration;
import pipelite.process.builder.ProcessBuilder;
import pipelite.stage.parameters.KubernetesExecutorParameters;
import pipelite.tester.TestType;
import pipelite.tester.entity.StageEntityAsserter;
import pipelite.tester.pipeline.ExecutorTestExitCode;
import pipelite.tester.pipeline.ExecutorTestParameters;

public class SingleStageKubernetesTestProcessConfiguration
    extends SingleStageTestProcessConfiguration {

  private final KubernetesTestConfiguration kubernetesTestConfiguration;

  public SingleStageKubernetesTestProcessConfiguration(
      TestType testType, KubernetesTestConfiguration kubernetesTestConfiguration) {
    super(
        testType,
        (stageService, pipelineName, processId, stageName) ->
            StageEntityAsserter.assertSubmittedKubernetesStageEntity(
                stageService,
                testType,
                kubernetesTestConfiguration,
                pipelineName,
                processId,
                stageName),
        (stageService, pipelineName, processId, stageName) ->
            StageEntityAsserter.assertCompletedKubernetesStageEntity(
                stageService,
                testType,
                kubernetesTestConfiguration,
                pipelineName,
                processId,
                stageName));
    this.kubernetesTestConfiguration = kubernetesTestConfiguration;
  }

  @Override
  protected void configure(ProcessBuilder builder) {
    KubernetesExecutorParameters params =
        ExecutorTestParameters.kubernetesParams(
            kubernetesTestConfiguration,
            immediateRetries(),
            maximumRetries(),
            testType().permanentErrors());
    int exitCode = testType().nextExitCode(pipelineName(), builder.getProcessId(), stageName());
    ExecutorTestExitCode.withKubernetesExecutor(builder.execute(stageName()), exitCode, params);
  }
}
