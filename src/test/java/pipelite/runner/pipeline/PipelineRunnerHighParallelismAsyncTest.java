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
import pipelite.manager.ProcessRunnerPoolManager;
import pipelite.metrics.PipeliteMetrics;
import pipelite.metrics.collector.ProcessRunnerMetrics;
import pipelite.process.builder.ProcessBuilder;
import pipelite.service.PipeliteServices;
import pipelite.stage.executor.StageExecutorState;
import pipelite.tester.pipeline.ConfigurableTestPipeline;
import pipelite.tester.process.TestProcessConfiguration;

@SpringBootTest(
    classes = PipeliteTestConfigWithManager.class,
    properties = {
      "pipelite.service.force=true",
      "pipelite.service.name=PipelineRunnerAsyncTest",
      "pipelite.advanced.shutdownIfIdle=true"
    })
@ActiveProfiles({"test", "PipelineRunnerHighParallelismAsyncTest"})
@DirtiesContext
public class PipelineRunnerHighParallelismAsyncTest {
  // Testing high pipeline parallelism with limited number of process runner
  // and stage runner workers.

  private static final int PROCESS_CNT = 20; // Increase process count for a more intensive test
  private static final int PARALLELISM = PROCESS_CNT;
  private static final Duration SUBMIT_TIME = Duration.ofSeconds(1);
  private static final Duration EXECUTION_TIME = Duration.ofSeconds(1);
  private static final String STAGE_NAME = "STAGE";

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

  public static class AsyncTestPipeline extends ConfigurableTestPipeline<TestProcessConfiguration> {
    public AsyncTestPipeline() {
      super(
          PARALLELISM,
          PROCESS_CNT,
          new TestProcessConfiguration() {
            @Override
            public void configureProcess(ProcessBuilder builder) {
              builder
                  .execute(STAGE_NAME)
                  .withAsyncTestExecutor(StageExecutorState.SUCCESS, SUBMIT_TIME, EXECUTION_TIME);
            }
          });
    }
  }

  private void test(AsyncTestPipeline pipeline) {
    String pipelineName = pipeline.pipelineName();
    PipelineRunner pipelineRunner = pipeliteServices.runner().getPipelineRunner(pipelineName).get();
    assertThat(pipelineRunner.getActiveProcessRunners().size()).isEqualTo(0);

    ProcessRunnerMetrics processRunnerMetrics = metrics.process(pipelineName);
    assertThat(metrics.error().count()).isEqualTo(0);
    // TODO: higher than expected completed count
    assertThat(processRunnerMetrics.completedCount()).isEqualTo(PROCESS_CNT);
    assertThat(processRunnerMetrics.failedCount()).isZero();
    assertThat(processRunnerMetrics.stage(STAGE_NAME).failedCount()).isEqualTo(0);
    assertThat(processRunnerMetrics.stage(STAGE_NAME).successCount()).isEqualTo(PROCESS_CNT);
  }

  @Test
  public void test() {
    long begin = System.currentTimeMillis();
    processRunnerPoolManager.createPools();
    processRunnerPoolManager.startPools();
    processRunnerPoolManager.waitPoolsToStop();

    for (AsyncTestPipeline pipeline : pipelines) {
      test(pipeline);
    }
    long end = System.currentTimeMillis();
    System.out.println("Test duration:" + (end - begin) / 1000 + "s");
  }
}
