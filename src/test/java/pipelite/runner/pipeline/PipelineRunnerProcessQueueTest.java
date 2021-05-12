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
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import pipelite.PipeliteTestConfigWithServices;
import pipelite.PrioritizedPipeline;
import pipelite.UniqueStringGenerator;
import pipelite.configuration.PipeliteConfiguration;
import pipelite.helper.PrioritizedPipelineTestHelper;
import pipelite.metrics.PipeliteMetrics;
import pipelite.process.builder.ProcessBuilder;
import pipelite.runner.process.ProcessQueue;
import pipelite.runner.process.ProcessRunner;
import pipelite.runner.process.creator.ProcessCreator;
import pipelite.service.PipeliteServices;
import pipelite.stage.executor.StageExecutorResult;
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

  private static final String PIPELINE_NAME =
      UniqueStringGenerator.randomPipelineName(PipelineRunnerProcessQueueTest.class);
  private static final int PROCESS_CNT = 10;
  private static final Duration PROCESS_QUEUE_REFRESH_FREQUENCY = Duration.ofDays(1);
  private static final AtomicInteger syncExecutionCount = new AtomicInteger();
  private static final AtomicInteger asyncExecutionCount = new AtomicInteger();

  @Test
  public void sync() {

    PrioritizedPipeline prioritizedPipeline =
        new PrioritizedPipelineTestHelper(PIPELINE_NAME, PROCESS_CNT) {

          @Override
          public int testConfigureParallelism() {
            return PROCESS_CNT;
          }

          @Override
          public void testConfigureProcess(ProcessBuilder builder) {
            builder
                .execute("STAGE")
                .withSyncTestExecutor(
                    (request) -> {
                      syncExecutionCount.incrementAndGet();
                      return StageExecutorResult.success();
                    });
          }
        };

    test(prioritizedPipeline, syncExecutionCount);
  }

  @Test
  public void async() {

    PrioritizedPipeline prioritizedPipeline =
        new PrioritizedPipelineTestHelper(PIPELINE_NAME, PROCESS_CNT) {

          @Override
          public int testConfigureParallelism() {
            return PROCESS_CNT;
          }

          @Override
          public void testConfigureProcess(ProcessBuilder builder) {
            builder
                .execute("STAGE")
                .withAsyncTestExecutor(
                    (request) -> {
                      asyncExecutionCount.incrementAndGet();
                      return StageExecutorResult.success();
                    });
          }
        };

    test(prioritizedPipeline, asyncExecutionCount);
  }

  private void test(PrioritizedPipeline prioritizedPipeline, AtomicInteger executionCount) {

    ProcessCreator processCreator =
        new ProcessCreator(prioritizedPipeline, pipeliteServices.process());

    ProcessQueue processQueue =
        spy(new ProcessQueue(pipeliteConfiguration, pipeliteServices, prioritizedPipeline));

    // Queue should be filled
    assertThat(processQueue.isRefreshQueue()).isTrue();
    // Queue should be empty
    assertThat(processQueue.getProcessQueueSize()).isZero();

    boolean lockProcess = true;
    PipelineRunner pipelineRunner =
        PipelineRunnerFactory.create(
            pipeliteConfiguration,
            pipeliteServices,
            pipeliteMetrics,
            prioritizedPipeline,
            processCreator,
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

    // Refresh is called two times. Once before creating processes when
    // the process queue is created empty and immediately after
    // creating processing.
    verify(processQueue, times(2)).refreshQueue();
    assertThat(executionCount.get()).isEqualTo(PROCESS_CNT);
    assertThat(processQueue.isRefreshQueue()).isFalse();
    assertThat(processQueue.getProcessQueueSize()).isZero();
  }
}
