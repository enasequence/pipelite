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
import pipelite.stage.executor.StageExecutorState;
import pipelite.stage.parameters.ExecutorParameters;
import pipelite.tester.TestType;
import pipelite.tester.TestTypeConfiguration;
import pipelite.tester.entity.StageEntityAsserter;

public class SingleStageAsyncTestProcessConfiguration extends SingleStageTestProcessConfiguration {

  private final StageExecutorState completedExecutorState;

  public SingleStageAsyncTestProcessConfiguration(TestTypeConfiguration testConfiguration) {
    super(
        testConfiguration,
        (stageService, pipelineName, processId, stageName) -> {},
        (stageService, pipelineName, processId, stageName) ->
            StageEntityAsserter.assertTestExecutorStageEntity(
                stageService, testConfiguration, pipelineName, processId, stageName));
    this.completedExecutorState =
        testConfiguration.testType() == TestType.SUCCESS
            ? StageExecutorState.SUCCESS
            : StageExecutorState.ERROR;
  }

  @Override
  protected void configure(ProcessBuilder builder) {
    ExecutorParameters.ExecutorParametersBuilder<?, ?> executorParamsBuilder =
        ExecutorParameters.builder();
    executorParamsBuilder.maximumRetries(maximumRetries()).immediateRetries(immediateRetries());
    testExecutorParams(executorParamsBuilder);
    ExecutorParameters executorParams = executorParamsBuilder.build();
    builder.execute(stageName()).withAsyncTestExecutor(completedExecutorState, executorParams);
  }

  protected void testExecutorParams(
      ExecutorParameters.ExecutorParametersBuilder<?, ?> executorParamsBuilder) {}
}
