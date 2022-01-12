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
import pipelite.configuration.properties.LsfTestConfiguration;
import pipelite.helper.TestType;
import pipelite.helper.entity.StageEntityTestHelper;
import pipelite.process.builder.ProcessBuilder;
import pipelite.stage.parameters.SimpleLsfExecutorParameters;

public class SingleStageSimpleLsfTestProcessConfiguration
    extends SingleStageTestProcessConfiguration<SingleStageSimpleLsfTestProcessConfiguration> {

  private final LsfTestConfiguration lsfTestConfiguration;

  public SingleStageSimpleLsfTestProcessConfiguration(
      TestType testType,
      int immediateRetries,
      int maximumRetries,
      LsfTestConfiguration lsfTestConfiguration) {
    super(
        testType,
        immediateRetries,
        maximumRetries,
        (stageService, pipelineName, processId, stageName) ->
            StageEntityTestHelper.assertSubmittedSimpleLsfExecutorStageEntity(
                testType,
                stageService,
                lsfTestConfiguration,
                pipelineName,
                processId,
                stageName,
                immediateRetries,
                maximumRetries),
        (stageService, pipelineName, processId, stageName) ->
            StageEntityTestHelper.assertCompletedSimpleLsfExecutorStageEntity(
                testType,
                stageService,
                lsfTestConfiguration,
                pipelineName,
                processId,
                stageName,
                immediateRetries,
                maximumRetries));
    this.lsfTestConfiguration = lsfTestConfiguration;
  }

  @Override
  protected void testConfigureProcess(ProcessBuilder builder) {
    SimpleLsfExecutorParameters.SimpleLsfExecutorParametersBuilder<?, ?> executorParamsBuilder =
        SimpleLsfExecutorParameters.builder();
    executorParamsBuilder
        .user(lsfTestConfiguration.getUser())
        .host(lsfTestConfiguration.getHost())
        .workDir(lsfTestConfiguration.getWorkDir())
        .timeout(Duration.ofSeconds(180))
        .maximumRetries(maximumRetries())
        .immediateRetries(immediateRetries());
    testExecutorParams(executorParamsBuilder);
    SimpleLsfExecutorParameters executorParams = executorParamsBuilder.build();
    executorParams.setPermanentErrors(testType().permanentErrors());
    builder.execute(stageName()).withSimpleLsfExecutor(testType().cmd(), executorParams);
  }

  protected void testExecutorParams(
      SimpleLsfExecutorParameters.SimpleLsfExecutorParametersBuilder<?, ?> executorParamsBuilder) {}
}
