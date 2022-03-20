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
import org.junit.jupiter.api.Disabled;
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
import pipelite.metrics.ProcessMetrics;
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
      "pipelite.advanced.processRunnerFrequency=2s",
      "pipelite.advanced.shutdownIfIdle=true",
      "pipelite.advanced.processRunnerWorkers=5",
      "pipelite.advanced.stageRunnerWorkers=5",
      "pipelite.repository.maxActive=10"
    })
@ActiveProfiles({"test", "PipelineRunnerHighParallelismSyncTest"})
@DirtiesContext
public class PipelineRunnerHighParallelismSyncTest {
  // Testing high pipeline parallelism with limited number of process runner
  // and stage runner workers.
  // PipelineRunnerHighParallelismAsyncTest is significantly faster than
  // PipelineRunnerHighParallelismSyncTest. Both have identical configuration except
  // for the asynchronous vs synchronous stage execution.

  private static final int PROCESS_CNT = 20; // Increase process count for a more intensive test
  private static final int PARALLELISM = Integer.MAX_VALUE;
  private static final Duration EXECUTION_TIME = Duration.ofSeconds(5);
  private static final String STAGE_NAME = "STAGE";

  @Autowired private ProcessRunnerPoolManager processRunnerPoolManager;
  @Autowired private PipeliteServices pipeliteServices;
  @Autowired private PipeliteMetrics metrics;

  @Autowired private List<SyncTestPipeline> pipelines;

  @Profile("PipelineRunnerHighParallelismSyncTest")
  @TestConfiguration
  static class TestConfig {
    @Bean
    public SyncTestPipeline syncTestPipeline1() {
      return new SyncTestPipeline();
    }

    @Bean
    public SyncTestPipeline syncTestPipeline2() {
      return new SyncTestPipeline();
    }

    @Bean
    public SyncTestPipeline syncTestPipeline3() {
      return new SyncTestPipeline();
    }

    @Bean
    public SyncTestPipeline syncTestPipeline4() {
      return new SyncTestPipeline();
    }

    @Bean
    public SyncTestPipeline syncTestPipeline5() {
      return new SyncTestPipeline();
    }
  }

  public static class SyncTestPipeline extends ConfigurableTestPipeline<TestProcessConfiguration> {
    public SyncTestPipeline() {
      super(
          PARALLELISM,
          PROCESS_CNT,
          new TestProcessConfiguration() {
            @Override
            public void configure(ProcessBuilder builder) {
              builder
                  .execute(STAGE_NAME)
                  .withSyncTestExecutor(StageExecutorState.SUCCESS, EXECUTION_TIME);
            }
          });
    }
  }

  private void test(SyncTestPipeline pipeline) {
    String pipelineName = pipeline.pipelineName();
    PipelineRunner pipelineRunner = pipeliteServices.runner().getPipelineRunner(pipelineName).get();
    assertThat(pipelineRunner.getActiveProcessRunners().size()).isEqualTo(0);

    ProcessMetrics processMetrics = metrics.process(pipelineName);
    assertThat(metrics.error().count()).isEqualTo(0);
    // TODO: higher than expected completed count
    assertThat(processMetrics.runner().completedCount()).isEqualTo(PROCESS_CNT);
    assertThat(processMetrics.runner().failedCount()).isZero();
    assertThat(processMetrics.stage(STAGE_NAME).runner().failedCount()).isEqualTo(0);
    assertThat(processMetrics.stage(STAGE_NAME).runner().successCount()).isEqualTo(PROCESS_CNT);
  }

  @Test
  @Disabled
  public void test() {
    processRunnerPoolManager.createPools();
    processRunnerPoolManager.startPools();
    processRunnerPoolManager.waitPoolsToStop();

    for (SyncTestPipeline pipeline : pipelines) {
      test(pipeline);
    }
  }
}
