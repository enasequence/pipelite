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

import pipelite.process.builder.ProcessBuilder;
import pipelite.stage.parameters.CmdExecutorParameters;
import pipelite.tester.TestType;
import pipelite.tester.entity.StageEntityAsserter;

public class SingleStageCmdTestProcessConfiguration extends SingleStageTestProcessConfiguration {

  public SingleStageCmdTestProcessConfiguration(TestType testType) {
    super(
        testType,
        (stageService, pipelineName, processId, stageName) -> {},
        (stageService, pipelineName, processId, stageName) ->
            StageEntityAsserter.assertCompletedCmdStageEntity(
                stageService, testType, pipelineName, processId, stageName));
  }

  @Override
  protected void configure(ProcessBuilder builder) {
    CmdExecutorParameters.CmdExecutorParametersBuilder<?, ?> executorParamsBuilder =
        CmdExecutorParameters.builder();
    executorParamsBuilder.maximumRetries(maximumRetries()).immediateRetries(immediateRetries());
    CmdExecutorParameters executorParams = executorParamsBuilder.build();
    executorParams.setPermanentErrors(testType().permanentErrors());
    builder
        .execute(stageName())
        .withCmdExecutor(
            testType().nextCmd(pipelineName(), builder.getProcessId(), stageName()),
            executorParams);
  }
}
