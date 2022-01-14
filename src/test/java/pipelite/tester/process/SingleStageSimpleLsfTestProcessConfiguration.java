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
import pipelite.configuration.properties.LsfTestConfiguration;
import pipelite.process.builder.ProcessBuilder;
import pipelite.stage.parameters.SimpleLsfExecutorParameters;
import pipelite.tester.TestTypeConfiguration;
import pipelite.tester.entity.StageEntityAsserter;

public class SingleStageSimpleLsfTestProcessConfiguration
    extends SingleStageTestProcessConfiguration {

  private final LsfTestConfiguration lsfTestConfiguration;

  public SingleStageSimpleLsfTestProcessConfiguration(
      TestTypeConfiguration testConfiguration, LsfTestConfiguration lsfTestConfiguration) {
    super(
        testConfiguration,
        (stageService, pipelineName, processId, stageName) ->
            StageEntityAsserter.assertSubmittedSimpleLsfStageEntity(
                stageService,
                testConfiguration,
                lsfTestConfiguration,
                pipelineName,
                processId,
                stageName),
        (stageService, pipelineName, processId, stageName) ->
            StageEntityAsserter.assertCompletedSimpleLsfStageEntity(
                stageService,
                testConfiguration,
                lsfTestConfiguration,
                pipelineName,
                processId,
                stageName));
    this.lsfTestConfiguration = lsfTestConfiguration;
  }

  @Override
  protected void configure(ProcessBuilder builder) {
    SimpleLsfExecutorParameters.SimpleLsfExecutorParametersBuilder<?, ?> executorParamsBuilder =
        SimpleLsfExecutorParameters.builder();
    executorParamsBuilder
        .user(lsfTestConfiguration.getUser())
        .host(lsfTestConfiguration.getHost())
        .workDir(lsfTestConfiguration.getWorkDir())
        .timeout(Duration.ofSeconds(180))
        // .saveLog(false)
        .maximumRetries(maximumRetries())
        .immediateRetries(immediateRetries());
    SimpleLsfExecutorParameters executorParams = executorParamsBuilder.build();
    executorParams.setPermanentErrors(
        testConfiguration()
            .nextPermanentErrors(pipelineName(), builder.getProcessId(), stageName()));
    builder
        .execute(stageName())
        .withSimpleLsfExecutor(
            testConfiguration().nextCmd(pipelineName(), builder.getProcessId(), stageName()),
            executorParams);
  }
}
