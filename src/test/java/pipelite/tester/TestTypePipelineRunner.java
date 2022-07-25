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
package pipelite.tester;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import pipelite.manager.ProcessRunnerPoolManager;
import pipelite.metrics.PipeliteMetrics;
import pipelite.runner.pipeline.PipelineRunner;
import pipelite.service.ProcessService;
import pipelite.service.RegisteredPipelineService;
import pipelite.service.RunnerService;
import pipelite.service.StageService;
import pipelite.tester.pipeline.ConfigurableTestPipeline;
import pipelite.tester.process.SingleStageTestProcessConfiguration;

@Component
public class TestTypePipelineRunner {

  @Autowired private RegisteredPipelineService registeredPipelineService;
  @Autowired private ProcessRunnerPoolManager processRunnerPoolManager;
  @Autowired private RunnerService runnerService;
  @Autowired private ProcessService processService;
  @Autowired private PipeliteMetrics metrics;

  public <T extends SingleStageTestProcessConfiguration> void runPipelines(
      StageService stageServiceSpy,
      int parallelism,
      int processCnt,
      Function<TestType, T> testProcessConfigurationFactory) {
    // Register test pipelines.
    List<ConfigurableTestPipeline<T>> testPipelines = new ArrayList<>();
    for (TestType testType : TestType.init()) {
      ConfigurableTestPipeline<T> pipeline =
          new ConfigurableTestPipeline<T>(
              parallelism, processCnt, testProcessConfigurationFactory.apply(testType));
      testPipelines.add(pipeline);
      registeredPipelineService.registerPipeline(pipeline);
    }

    // Spy stage service.
    TestType.spyStageService(stageServiceSpy);

    // Run test pipelines.
    processRunnerPoolManager.createPools();
    processRunnerPoolManager.startPools();
    processRunnerPoolManager.waitPoolsToStop();

    // Assert test pipelines.
    for (PipelineRunner pipelineRunner : runnerService.getPipelineRunners()) {
      assertThat(pipelineRunner.getActiveProcessRunners().size()).isEqualTo(0);
    }
    for (ConfigurableTestPipeline<T> testPipeline : testPipelines) {
      assertPipeline(stageServiceSpy, testPipeline, processCnt);
    }
  }

  private <T extends SingleStageTestProcessConfiguration> void assertPipeline(
      StageService stageServiceSpy, ConfigurableTestPipeline<T> testPipeline, int processCnt) {
    SingleStageTestProcessConfiguration testProcessConfiguration =
        testPipeline.testProcessConfiguration();
    assertThat(testPipeline.configuredProcessIds().size()).isEqualTo(processCnt);
    testProcessConfiguration.assertCompleted(processService, stageServiceSpy, metrics, processCnt);
  }
}
