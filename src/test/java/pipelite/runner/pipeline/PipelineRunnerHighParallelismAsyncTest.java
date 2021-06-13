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

import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Profile;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import pipelite.PipeliteTestConfigWithManager;
import pipelite.helper.RegisteredConfiguredTestPipeline;
import pipelite.helper.RegisteredTestPipelineWrappingPipeline;
import pipelite.manager.ProcessRunnerPoolManager;
import pipelite.metrics.PipelineMetrics;
import pipelite.metrics.PipeliteMetrics;
import pipelite.process.builder.ProcessBuilder;
import pipelite.service.PipeliteServices;
import pipelite.stage.executor.StageExecutorState;

@SpringBootTest(
    classes = PipeliteTestConfigWithManager.class,
    properties = {
      "pipelite.service.force=true",
      "pipelite.service.name=PipelineRunnerAsyncTest",
      "pipelite.advanced.processRunnerFrequency=2s",
      "pipelite.advanced.shutdownIfIdle=true",
      "pipelite.advanced.processRunnerWorkers=5",
      "pipelite.advanced.stageRunnerWorkers=5",
      "pipelite.repository.maxActive=10"
    })
@ActiveProfiles({"test", "PipelineRunnerHighParallelismAsyncTest"})
@DirtiesContext
public class PipelineRunnerHighParallelismAsyncTest {
  // Testing high pipeline parallelism with limited number of process runner
  // and stage runner workers.
  // PipelineRunnerHighParallelismAsyncTest is significantly faster than
  // PipelineRunnerHighParallelismSyncTest. Both have identical configuration except
  // for the asynchronous vs synchronous stage execution.

  private static final int PROCESS_CNT = 20; // Increase process count for a more intensive test
  private static final int PARALLELISM = Integer.MAX_VALUE;
  private static final Duration EXECUTION_TIME = Duration.ofSeconds(5);

  @Autowired private ProcessRunnerPoolManager processRunnerPoolManager;
  @Autowired private PipeliteServices pipeliteServices;
  @Autowired private PipeliteMetrics metrics;

  @Autowired private List<AsyncTestPipeline> pipelines;

  @Profile("PipelineRunnerHighParallelismAsyncTest")
  @TestConfiguration
  static class TestConfig {
    @Bean
    public AsyncTestPipeline asyncTestPipeline1() {
      return new AsyncTestPipeline();
    }

    @Bean
    public AsyncTestPipeline asyncTestPipeline2() {
      return new AsyncTestPipeline();
    }

    @Bean
    public AsyncTestPipeline asyncTestPipeline3() {
      return new AsyncTestPipeline();
    }

    @Bean
    public AsyncTestPipeline asyncTestPipeline4() {
      return new AsyncTestPipeline();
    }

    @Bean
    public AsyncTestPipeline asyncTestPipeline5() {
      return new AsyncTestPipeline();
    }
  }

  public static class AsyncTestPipeline
      extends RegisteredTestPipelineWrappingPipeline<RegisteredConfiguredTestPipeline> {
    public AsyncTestPipeline() {
      super(
          PARALLELISM,
          PROCESS_CNT,
          new RegisteredConfiguredTestPipeline() {
            @Override
            public void testConfigureProcess(ProcessBuilder builder) {
              builder
                  .execute("STAGE")
                  .withAsyncTestExecutor(StageExecutorState.SUCCESS, EXECUTION_TIME);
            }
          });
    }
  }

  private void test(AsyncTestPipeline pipeline) {
    String pipelineName = pipeline.pipelineName();
    PipelineRunner pipelineRunner = pipeliteServices.runner().getPipelineRunner(pipelineName).get();
    assertThat(pipelineRunner.getActiveProcessRunners().size()).isEqualTo(0);

    PipelineMetrics pipelineMetrics = metrics.pipeline(pipelineName);
    assertThat(pipelineMetrics.process().getInternalErrorCount()).isEqualTo(0);
    // TODO: higher than expected completed count
    assertThat(pipelineMetrics.process().getCompletedCount()).isEqualTo(PROCESS_CNT);
    assertThat(pipelineMetrics.process().getFailedCount()).isZero();
    assertThat(pipelineMetrics.stage().getFailedCount()).isEqualTo(0);
    assertThat(pipelineMetrics.stage().getSuccessCount()).isEqualTo(PROCESS_CNT);
  }

  @Test
  public void test() {
    processRunnerPoolManager.createPools();
    processRunnerPoolManager.startPools();
    processRunnerPoolManager.waitPoolsToStop();

    for (AsyncTestPipeline pipeline : pipelines) {
      test(pipeline);
    }
  }
}
