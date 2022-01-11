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

import pipelite.helper.TestType;
import pipelite.helper.entity.StageEntityTestHelper;
import pipelite.process.builder.ProcessBuilder;
import pipelite.stage.executor.StageExecutorState;
import pipelite.stage.parameters.ExecutorParameters;

public class SingleStageSyncTestProcessConfiguration
    extends SingleStageTestProcessConfiguration<SingleStageSyncTestProcessConfiguration> {

  private final StageExecutorState completedExecutorState;
  private ExecutorParameters executorParams;

  public SingleStageSyncTestProcessConfiguration(
      TestType testType, int immediateRetries, int maximumRetries) {
    super(
        testType,
        immediateRetries,
        maximumRetries,
        (stageService, pipelineName, processId, stageName, thisPipeline) -> {},
        (stageService, pipelineName, processId, stageName, thisPipeline) ->
            StageEntityTestHelper.assertCompletedTestExecutorStageEntity(
                testType,
                stageService,
                pipelineName,
                processId,
                stageName,
                immediateRetries,
                maximumRetries));
    this.completedExecutorState =
        testType == TestType.SUCCESS ? StageExecutorState.SUCCESS : StageExecutorState.ERROR;
  }

  @Override
  protected void testConfigureProcess(ProcessBuilder builder) {
    ExecutorParameters.ExecutorParametersBuilder<?, ?> executorParamsBuilder =
        ExecutorParameters.builder();
    executorParamsBuilder.maximumRetries(maximumRetries()).immediateRetries(immediateRetries());
    testExecutorParams(executorParamsBuilder);
    executorParams = executorParamsBuilder.build();
    builder.execute(stageName()).withSyncTestExecutor(completedExecutorState, executorParams);
  }

  protected void testExecutorParams(
      ExecutorParameters.ExecutorParametersBuilder<?, ?> executorParamsBuilder) {}

  public StageExecutorState getCompletedExecutorState() {
    return completedExecutorState;
  }

  public ExecutorParameters executorParams() {
    return executorParams;
  }
}
