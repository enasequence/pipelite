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
import java.time.ZonedDateTime;
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
import pipelite.runner.process.creator.DefaultPrioritizedProcessCreator;
import pipelite.runner.process.creator.PrioritizedProcessCreator;
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
      "pipelite.advanced.processQueueMinRefreshFrequency=24h",
      "pipelite.advanced.processQueueMaxRefreshFrequency=24h",
      "pipelite.advanced.processCreateMaxSize=10"
    })
@ActiveProfiles({"test"})
@DirtiesContext
public class PipelineRunnerDefaultQueueTest {

  @Autowired private PipeliteConfiguration pipeliteConfiguration;
  @Autowired private PipeliteServices pipeliteServices;
  @Autowired private PipeliteMetrics pipeliteMetrics;

  private static final String PIPELINE_NAME =
      UniqueStringGenerator.randomPipelineName(PipelineRunnerDefaultQueueTest.class);
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

    PrioritizedProcessCreator prioritizedProcessCreator =
        new DefaultPrioritizedProcessCreator(prioritizedPipeline, pipeliteServices.process());

    ProcessQueue queue =
        spy(
            new ProcessQueue(
                pipeliteConfiguration.advanced(),
                pipeliteServices.process(),
                prioritizedPipeline.pipelineName(),
                prioritizedPipeline.configurePipeline().pipelineParallelism()));

    // Queue should be filled
    assertThat(queue.isFillQueue()).isTrue();
    // Queue should be empty
    assertThat(queue.isAvailableProcesses(0)).isFalse();
    assertThat(queue.getQueuedProcessCount()).isZero();
    // There should be no processes in active state waiting to be executed
    assertThat(queue.getUnlockedActiveProcesses().size()).isEqualTo(0);
    // There should be no processes in pending state waiting to be executed
    assertThat(queue.getPendingProcesses().size()).isEqualTo(0);

    // Queue max valid time should be before now
    assertThat(queue.getProcessQueueMaxValidUntil()).isBeforeOrEqualTo(ZonedDateTime.now());
    assertThat(queue.getProcessQueueMinValidUntil()).isBeforeOrEqualTo(ZonedDateTime.now());

    PipelineRunner pipelineRunner =
        new PipelineRunner(
            pipeliteConfiguration,
            pipeliteServices,
            pipeliteMetrics,
            prioritizedPipeline,
            prioritizedProcessCreator,
            queue,
            (pipelineName1, process1) ->
                new ProcessRunner(
                    pipeliteConfiguration,
                    pipeliteServices,
                    pipeliteMetrics,
                    pipelineName1,
                    process1));

    ZonedDateTime expectedValidLowerBound =
        ZonedDateTime.now().plus(PROCESS_QUEUE_REFRESH_FREQUENCY);

    // Execute processes from the queue
    pipelineRunner.runOneIteration();

    // Check that the queue was filled
    verify(queue, times(1)).fillQueue();

    ZonedDateTime expectedValidUpperBound =
        ZonedDateTime.now().plus(PROCESS_QUEUE_REFRESH_FREQUENCY);

    // Queue max valid time should be between expected bounds
    assertThat(queue.getProcessQueueMaxValidUntil())
        .isBetween(expectedValidLowerBound, expectedValidUpperBound);
    assertThat(queue.getProcessQueueMinValidUntil())
        .isBetween(expectedValidLowerBound, expectedValidUpperBound);

    // Wait for the processes to execute
    while (pipelineRunner.getActiveProcessCount() > 0) {
      Time.wait(Duration.ofSeconds(1));
      pipelineRunner.runOneIteration();
    }

    // Check that all processes were executed
    assertThat(executionCount.get()).isEqualTo(PROCESS_CNT);

    // Queue should not be filled because refresh frequency is 24 hours
    assertThat(queue.isFillQueue()).isFalse();
    // Queue should be empty
    assertThat(queue.isAvailableProcesses(0)).isFalse();
    assertThat(queue.getQueuedProcessCount()).isZero();
    // There should be no processes in active state waiting to be executed
    assertThat(queue.getUnlockedActiveProcesses().size()).isEqualTo(0);
    // There should be no processes in pending state waiting to be executed
    assertThat(queue.getPendingProcesses().size()).isEqualTo(0);
  }
}