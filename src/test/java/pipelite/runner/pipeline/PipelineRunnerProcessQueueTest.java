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
package pipelite.runner.pipeline;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.internal.verification.VerificationModeFactory.times;

import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import pipelite.Pipeline;
import pipelite.PipeliteTestConfigWithServices;
import pipelite.configuration.PipeliteConfiguration;
import pipelite.metrics.PipeliteMetrics;
import pipelite.process.builder.ProcessBuilder;
import pipelite.runner.process.ProcessQueue;
import pipelite.runner.process.ProcessRunner;
import pipelite.runner.process.creator.ProcessEntityCreator;
import pipelite.service.PipeliteServices;
import pipelite.stage.executor.StageExecutorState;
import pipelite.tester.pipeline.ConfigurableTestPipeline;
import pipelite.tester.process.TestProcessConfiguration;
import pipelite.time.Time;

/**
 * DefaultProcessQueue integration test using PipelineRunner. PipelineRunner will be executed for
 * one iteration. Ten processes will be created and executed in parallel.
 */
@SpringBootTest(
    classes = PipeliteTestConfigWithServices.class,
    properties = {
      "pipelite.service.force=true",
      "pipelite.service.name=PipelineRunnerDefaultProcessQueueTest",
      "pipelite.advanced.processRunnerFrequency=250ms",
      "pipelite.advanced.processQueueMinRefreshFrequency=24h",
      "pipelite.advanced.processQueueMaxRefreshFrequency=24h",
      "pipelite.advanced.processCreateMaxSize=10",
      "pipelite.advanced.shutdownIfIdle=true"
    })
@ActiveProfiles({"test"})
@DirtiesContext
public class PipelineRunnerProcessQueueTest {

  @Autowired private PipeliteConfiguration pipeliteConfiguration;
  @Autowired private PipeliteServices pipeliteServices;
  @Autowired private PipeliteMetrics pipeliteMetrics;

  private static final int PARALLELISM = 10;
  private static final int PROCESS_CNT = 10;
  private static final String STAGE_NAME = "STAGE";

  private static final class SyncTestProcessConfiguration extends TestProcessConfiguration {
    @Override
    public void configureProcess(ProcessBuilder builder) {
      builder.execute(STAGE_NAME).withSyncTestExecutor(StageExecutorState.SUCCESS);
    }
  }

  private static final class AsyncTestProcessConfiguration extends TestProcessConfiguration {
    @Override
    public void configureProcess(ProcessBuilder builder) {
      builder.execute(STAGE_NAME).withAsyncTestExecutor(StageExecutorState.SUCCESS, null, null);
    }
  }

  @Test
  public void sync() {
    Pipeline pipeline =
        new ConfigurableTestPipeline<>(
            PARALLELISM, PROCESS_CNT, new SyncTestProcessConfiguration());
    test(pipeline);
  }

  @Test
  public void async() {
    Pipeline pipeline =
        new ConfigurableTestPipeline<>(
            PARALLELISM, PROCESS_CNT, new AsyncTestProcessConfiguration());
    test(pipeline);
  }

  private void test(Pipeline pipeline) {

    ProcessEntityCreator processEntityCreator =
        new ProcessEntityCreator(pipeline, pipeliteServices.process());

    ProcessQueue processQueue =
        spy(
            new ProcessQueue(
                pipeliteConfiguration, pipeliteServices, processEntityCreator, pipeline));

    // Queue should be filled
    assertThat(processQueue.isRefreshQueue()).isTrue();
    // Queue should be empty
    assertThat(processQueue.getCurrentQueueSize()).isZero();

    boolean lockProcess = true;
    PipelineRunner pipelineRunner =
        PipelineRunnerFactory.create(
            pipeliteConfiguration,
            pipeliteServices,
            pipeliteMetrics,
            pipeline,
            (pipeline1) -> processQueue,
            (pipelineName1, process1) ->
                new ProcessRunner(
                    pipeliteConfiguration,
                    pipeliteServices,
                    pipeliteMetrics,
                    pipelineName1,
                    process1,
                    lockProcess));

    // Wait for the processes to execute
    while (!pipelineRunner.isIdle()) {
      Time.wait(Duration.ofSeconds(1));
      pipelineRunner.runOneIteration();
    }

    verify(processQueue, times(1)).refreshQueue();
    assertThat(
            pipeliteMetrics
                .process(pipeline.pipelineName())
                .stage(STAGE_NAME)
                .runner()
                .successCount())
        .isEqualTo(PROCESS_CNT);
    assertThat(processQueue.isRefreshQueue()).isFalse();
    assertThat(processQueue.getCurrentQueueSize()).isZero();
  }
}
