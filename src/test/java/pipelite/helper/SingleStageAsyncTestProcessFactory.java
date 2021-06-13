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

import pipelite.process.builder.ProcessBuilder;
import pipelite.service.StageService;
import pipelite.stage.executor.StageExecutorState;
import pipelite.stage.parameters.ExecutorParameters;

public class SingleStageAsyncTestProcessFactory extends SingleStageTestProcessFactory {

  private final StageExecutorState completedExecutorState;
  private ExecutorParameters executorParams;

  public SingleStageAsyncTestProcessFactory(
      TestType testType,
      int processCnt,
      int parallelism,
      StageExecutorState completedExecutorState,
      int immediateRetries,
      int maximumRetries) {
    super(testType, processCnt, parallelism, immediateRetries, maximumRetries);
    this.completedExecutorState = completedExecutorState;
  }

  @Override
  protected void testConfigureProcess(ProcessBuilder builder) {
    ExecutorParameters.ExecutorParametersBuilder<?, ?> executorParamsBuilder =
        ExecutorParameters.builder();
    executorParamsBuilder.maximumRetries(maximumRetries()).immediateRetries(immediateRetries());
    testExecutorParams(executorParamsBuilder);
    executorParams = executorParamsBuilder.build();
    builder.execute(stageName()).withAsyncTestExecutor(completedExecutorState, executorParams);
  }

  protected void testExecutorParams(
      ExecutorParameters.ExecutorParametersBuilder<?, ?> executorParamsBuilder) {}

  public StageExecutorState getCompletedExecutorState() {
    return completedExecutorState;
  }

  @Override
  public void assertSubmittedStageEntity(StageService stageService, String processId) {}

  @Override
  public void assertCompletedStageEntity(StageService stageService, String processId) {
    StageEntityTestHelper.assertCompletedTestExecutorStageEntity(
        testType(),
        stageService,
        pipelineName(),
        processId,
        stageName(),
        immediateRetries(),
        maximumRetries());
  }
}
