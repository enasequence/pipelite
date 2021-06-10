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
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Profile;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import pipelite.PipeliteTestConfigWithManager;
import pipelite.UniqueStringGenerator;
import pipelite.helper.CreateProcessPipelineTestHelper;
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
  private static final int PIPELINE_PARALLELISM = Integer.MAX_VALUE;
  private static final Duration EXECUTION_TIME = Duration.ofSeconds(5);
  private static final String PIPELINE_NAME_1 =
      UniqueStringGenerator.randomPipelineName(PipelineRunnerHighParallelismAsyncTest.class);
  private static final String PIPELINE_NAME_2 =
      UniqueStringGenerator.randomPipelineName(PipelineRunnerHighParallelismAsyncTest.class);
  private static final String PIPELINE_NAME_3 =
      UniqueStringGenerator.randomPipelineName(PipelineRunnerHighParallelismAsyncTest.class);
  private static final String PIPELINE_NAME_4 =
      UniqueStringGenerator.randomPipelineName(PipelineRunnerHighParallelismAsyncTest.class);
  private static final String PIPELINE_NAME_5 =
      UniqueStringGenerator.randomPipelineName(PipelineRunnerHighParallelismAsyncTest.class);

  @Autowired private ProcessRunnerPoolManager processRunnerPoolManager;
  @Autowired private PipeliteServices pipeliteServices;
  @Autowired private PipeliteMetrics metrics;

  @Autowired private AsyncTestPipeline asyncTestPipeline1;
  @Autowired private AsyncTestPipeline asyncTestPipeline2;
  @Autowired private AsyncTestPipeline asyncTestPipeline3;
  @Autowired private AsyncTestPipeline asyncTestPipeline4;
  @Autowired private AsyncTestPipeline asyncTestPipeline5;

  @Profile("PipelineRunnerHighParallelismAsyncTest")
  @TestConfiguration
  static class TestConfig {
    @Bean
    public AsyncTestPipeline asyncTestPipeline1() {
      return new AsyncTestPipeline(PIPELINE_NAME_1);
    }

    @Bean
    public AsyncTestPipeline asyncTestPipeline2() {
      return new AsyncTestPipeline(PIPELINE_NAME_2);
    }

    @Bean
    public AsyncTestPipeline asyncTestPipeline3() {
      return new AsyncTestPipeline(PIPELINE_NAME_3);
    }

    @Bean
    public AsyncTestPipeline asyncTestPipeline4() {
      return new AsyncTestPipeline(PIPELINE_NAME_4);
    }

    @Bean
    public AsyncTestPipeline asyncTestPipeline5() {
      return new AsyncTestPipeline(PIPELINE_NAME_5);
    }
  }

  public static class AsyncTestPipeline extends CreateProcessPipelineTestHelper {
    public AsyncTestPipeline(String pipelineName) {
      super(pipelineName, PROCESS_CNT);
    }

    @Override
    public int testConfigureParallelism() {
      return PIPELINE_PARALLELISM;
    }

    @Override
    public void testConfigureProcess(ProcessBuilder builder) {
      builder.execute("STAGE").withAsyncTestExecutor(StageExecutorState.SUCCESS, EXECUTION_TIME);
    }
  }

  private void test(String pipelineName) {
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

    test(PIPELINE_NAME_1);
    test(PIPELINE_NAME_2);
    test(PIPELINE_NAME_3);
    test(PIPELINE_NAME_4);
    test(PIPELINE_NAME_5);
  }
}
