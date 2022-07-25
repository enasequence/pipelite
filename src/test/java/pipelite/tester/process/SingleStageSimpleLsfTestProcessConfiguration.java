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
package pipelite.tester.process;

import pipelite.configuration.properties.LsfTestConfiguration;
import pipelite.process.builder.ProcessBuilder;
import pipelite.stage.parameters.SimpleLsfExecutorParameters;
import pipelite.tester.TestType;
import pipelite.tester.entity.StageEntityAsserter;
import pipelite.tester.pipeline.ExecutorTestExitCode;
import pipelite.tester.pipeline.ExecutorTestParameters;

public class SingleStageSimpleLsfTestProcessConfiguration
    extends SingleStageTestProcessConfiguration {

  private final LsfTestConfiguration lsfTestConfiguration;

  public SingleStageSimpleLsfTestProcessConfiguration(
      TestType testType, LsfTestConfiguration lsfTestConfiguration) {
    super(
        testType,
        (stageService, pipelineName, processId, stageName) ->
            StageEntityAsserter.assertSubmittedSimpleLsfStageEntity(
                stageService, testType, lsfTestConfiguration, pipelineName, processId, stageName),
        (stageService, pipelineName, processId, stageName) ->
            StageEntityAsserter.assertCompletedSimpleLsfStageEntity(
                stageService, testType, lsfTestConfiguration, pipelineName, processId, stageName));
    this.lsfTestConfiguration = lsfTestConfiguration;
  }

  @Override
  protected void configure(ProcessBuilder builder) {
    SimpleLsfExecutorParameters params =
        ExecutorTestParameters.simpleLsfParams(
            lsfTestConfiguration,
            immediateRetries(),
            maximumRetries(),
            testType().permanentErrors());
    int exitCode = testType().nextExitCode(pipelineName(), builder.getProcessId(), stageName());
    ExecutorTestExitCode.withSimpleLsfExecutor(builder.execute(stageName()), exitCode, params);
  }
}
