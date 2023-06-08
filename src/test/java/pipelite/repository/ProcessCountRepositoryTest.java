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
package pipelite.repository;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import pipelite.manager.ProcessRunnerPoolManager;
import pipelite.process.builder.ProcessBuilder;
import pipelite.service.RegisteredPipelineService;
import pipelite.stage.executor.StageExecutorState;
import pipelite.test.PipeliteTestIdCreator;
import pipelite.test.configuration.PipeliteTestConfigWithManager;
import pipelite.tester.pipeline.ConfigurableTestPipeline;
import pipelite.tester.process.TestProcessConfiguration;

@SpringBootTest(
    webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
    classes = PipeliteTestConfigWithManager.class,
    properties = {
      "pipelite.service.force=true",
      "pipelite.service.name=ProcessCountRepositoryTest",
      "pipelite.advanced.processRunnerFrequency=2s",
      "pipelite.advanced.shutdownIfIdle=true"
    })
public class ProcessCountRepositoryTest {

  private static final String PIPELINE_NAME = PipeliteTestIdCreator.pipelineName();
  private static final String STAGE_NAME = PipeliteTestIdCreator.stageName();

  private static final int PARALLELISM = 5;
  private static final int PROCESS_CNT = 5;

  @Autowired private RegisteredPipelineService registeredPipelineService;

  @Autowired ProcessRunnerPoolManager processRunnerPoolManager;
  @Autowired private ProcessCountRepository repository;

  private TestProcessConfiguration failed =
      new TestProcessConfiguration(PIPELINE_NAME) {
        @Override
        public void configureProcess(ProcessBuilder builder) {
          builder.execute(STAGE_NAME).withAsyncTestExecutor(StageExecutorState.EXECUTION_ERROR);
        }
      };

  @Test
  public void test() {
    assertThat(repository.findProcessBacklogCount(PIPELINE_NAME)).isEqualTo(0);
    assertThat(repository.findProcessFailedCount(PIPELINE_NAME)).isEqualTo(0);

    ConfigurableTestPipeline pipeline =
        new ConfigurableTestPipeline(PARALLELISM, PROCESS_CNT, failed);
    registeredPipelineService.registerPipeline(pipeline);

    // Run test pipelines.
    processRunnerPoolManager.createPools();
    processRunnerPoolManager.startPools();
    processRunnerPoolManager.waitPoolsToStop();

    assertThat(repository.findProcessBacklogCount(PIPELINE_NAME)).isEqualTo(0);
    assertThat(repository.findProcessFailedCount(PIPELINE_NAME)).isEqualTo(5);
  }
}
