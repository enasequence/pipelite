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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;

import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.mockito.stubbing.Answer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Profile;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.transaction.annotation.Transactional;
import pipelite.configuration.ServiceConfiguration;
import pipelite.exception.PipeliteException;
import pipelite.executor.AsyncTestExecutor;
import pipelite.executor.describe.DescribeJobs;
import pipelite.executor.describe.cache.AsyncTestDescribeJobsCache;
import pipelite.executor.describe.context.executor.AsyncTestExecutorContext;
import pipelite.executor.describe.context.request.AsyncTestRequestContext;
import pipelite.manager.ProcessRunnerPoolManager;
import pipelite.metrics.PipeliteMetrics;
import pipelite.metrics.collector.ProcessRunnerMetrics;
import pipelite.process.builder.ProcessBuilder;
import pipelite.service.PipeliteServices;
import pipelite.service.RunnerService;
import pipelite.stage.executor.StageExecutorState;
import pipelite.stage.parameters.ExecutorParameters;
import pipelite.test.PipeliteTestIdCreator;
import pipelite.test.configuration.PipeliteTestConfigWithManager;
import pipelite.tester.pipeline.ConfigurableTestPipeline;
import pipelite.tester.process.TestProcessConfiguration;

/**
 * Tests DescribeJobs.getResults() and DescribeJobs.retrieveResults() exception handling behaviour.
 * The stage execution is expected to fail if DescribeJobs.getResult() throws an exception. The
 * stage execution is expected to continue if an exception is thrown in
 * DescribeJobs.retrieveResults().
 */
@SpringBootTest(
    classes = PipeliteTestConfigWithManager.class,
    properties = {
      "pipelite.service.force=true",
      "pipelite.advanced.shutdownIfIdle=true",
      "pipelite.service.name=DescribeJobsInternalErrorTest"
    })
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
@ActiveProfiles({"pipelite", "PipelineRunnerDescribeJobsGetResultExceptionTest"})
@Transactional
public class PipelineRunnerDescribeJobsGetResultExceptionTest {

  private static final int PROCESS_CNT = 2;
  private static final int PARALLELISM = 2;
  private static final String PIPELINE_NAME = PipeliteTestIdCreator.pipelineName();
  private static final String STAGE_NAME = PipeliteTestIdCreator.stageName();

  @SpyBean AsyncTestDescribeJobsCache asyncTestDescribeJobsCache;

  @Autowired ProcessRunnerPoolManager processRunnerPoolManager;
  @Autowired PipeliteServices pipeliteServices;
  @Autowired PipeliteMetrics pipeliteMetrics;
  @Autowired ServiceConfiguration serviceConfiguration;

  @Profile("PipelineRunnerDescribeJobsGetResultExceptionTest")
  @TestConfiguration
  static class TestConfig {

    @Bean
    public SingleStageAsyncTestPipeline singleStageAsyncTestPipeline() {
      return new SingleStageAsyncTestPipeline();
    }
  }

  public static class SingleStageAsyncTestPipeline
      extends ConfigurableTestPipeline<TestProcessConfiguration> {
    public SingleStageAsyncTestPipeline() {
      super(
          PARALLELISM,
          PROCESS_CNT,
          new TestProcessConfiguration(PIPELINE_NAME) {
            @Override
            public void configureProcess(ProcessBuilder builder) {
              ExecutorParameters params = new ExecutorParameters();
              params.setImmediateRetries(0);
              params.setMaximumRetries(0);
              builder.execute(STAGE_NAME).withAsyncTestExecutor(StageExecutorState.SUCCESS, params);
            }
          });
    }
  }

  @Test
  public void success() {

    processRunnerPoolManager.createPools();
    processRunnerPoolManager.startPools();
    processRunnerPoolManager.waitPoolsToStop();

    RunnerService runnerService = pipeliteServices.runner();
    assertThat(
            runnerService
                .getPipelineRunner(PIPELINE_NAME)
                .get()
                .getActiveProcessRunners()
                .isEmpty())
        .isTrue();

    ProcessRunnerMetrics processRunnerMetrics = pipeliteMetrics.process(PIPELINE_NAME);
    assertThat(processRunnerMetrics.completedCount()).isEqualTo(PROCESS_CNT);
    assertThat(processRunnerMetrics.failedCount()).isZero();
    assertThat(processRunnerMetrics.stage(STAGE_NAME).failedCount()).isEqualTo(0);
    assertThat(processRunnerMetrics.stage(STAGE_NAME).successCount()).isEqualTo(PROCESS_CNT);
  }

  // The stage execution is expected to fail if DescribeJobs.getResult() throws an exception.
  @Test
  public void exception() {

    AtomicInteger exceptionCount = new AtomicInteger();

    Answer<DescribeJobs<AsyncTestRequestContext, AsyncTestExecutorContext>>
        createDescribeJobsAnswer =
            invocation -> {
              AsyncTestExecutor executor = invocation.getArgument(0, AsyncTestExecutor.class);

              DescribeJobs<AsyncTestRequestContext, AsyncTestExecutorContext> describeJobs =
                  spy(
                      new DescribeJobs<>(
                          serviceConfiguration,
                          pipeliteServices.internalError(),
                          asyncTestDescribeJobsCache.requestLimit(),
                          asyncTestDescribeJobsCache.getExecutorContext(executor)));
              doAnswer(
                      invocation2 -> {
                        exceptionCount.incrementAndGet();
                        throw new PipeliteException("Expected exception from getResult");
                      })
                  .when(describeJobs)
                  .getResult(any());

              return describeJobs;
            };
    doAnswer(createDescribeJobsAnswer).when(asyncTestDescribeJobsCache).createDescribeJobs(any());

    processRunnerPoolManager.createPools();
    processRunnerPoolManager.startPools();
    processRunnerPoolManager.waitPoolsToStop();

    assertThat(exceptionCount.get()).isEqualTo(PROCESS_CNT);

    RunnerService runnerService = pipeliteServices.runner();
    assertThat(
            runnerService
                .getPipelineRunner(PIPELINE_NAME)
                .get()
                .getActiveProcessRunners()
                .isEmpty())
        .isTrue();

    ProcessRunnerMetrics processRunnerMetrics = pipeliteMetrics.process(PIPELINE_NAME);
    assertThat(processRunnerMetrics.failedCount()).isEqualTo(PROCESS_CNT);
    assertThat(processRunnerMetrics.completedCount()).isZero();
    assertThat(processRunnerMetrics.stage(STAGE_NAME).successCount()).isEqualTo(0);
    assertThat(processRunnerMetrics.stage(STAGE_NAME).failedCount()).isEqualTo(PROCESS_CNT);
    assertThat(pipeliteMetrics.error().count()).isEqualTo(PROCESS_CNT);
  }
}
