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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
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
import pipelite.PipeliteIdCreator;
import pipelite.PipeliteTestConfigWithManager;
import pipelite.configuration.ServiceConfiguration;
import pipelite.configuration.properties.KubernetesTestConfiguration;
import pipelite.configuration.properties.LsfTestConfiguration;
import pipelite.exception.PipeliteException;
import pipelite.executor.AbstractLsfExecutor;
import pipelite.executor.KubernetesExecutor;
import pipelite.executor.describe.DescribeJobs;
import pipelite.executor.describe.cache.KubernetesDescribeJobsCache;
import pipelite.executor.describe.cache.LsfDescribeJobsCache;
import pipelite.executor.describe.context.DefaultRequestContext;
import pipelite.executor.describe.context.KubernetesExecutorContext;
import pipelite.executor.describe.context.LsfExecutorContext;
import pipelite.executor.describe.context.LsfRequestContext;
import pipelite.manager.ProcessRunnerPoolManager;
import pipelite.metrics.PipeliteMetrics;
import pipelite.metrics.ProcessMetrics;
import pipelite.process.builder.ProcessBuilder;
import pipelite.service.PipeliteServices;
import pipelite.service.RunnerService;
import pipelite.stage.parameters.AbstractLsfExecutorParameters;
import pipelite.stage.parameters.KubernetesExecutorParameters;
import pipelite.stage.parameters.SimpleLsfExecutorParameters;
import pipelite.tester.pipeline.ConfigurableTestPipeline;
import pipelite.tester.pipeline.ExecutorTestExitCode;
import pipelite.tester.pipeline.ExecutorTestParameters;
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
@ActiveProfiles({"test", "PipelineRunnerDescribeJobsGetResultExceptionTest"})
@Transactional
public class PipelineRunnerDescribeJobsGetResultExceptionTest {

  private static final int PROCESS_CNT = 2;
  private static final int PARALLELISM = 2;
  private static final String PIPELINE_NAME_SIMPLE_LSF = PipeliteIdCreator.pipelineName();
  private static final String PIPELINE_NAME_KUBERNETES = PipeliteIdCreator.pipelineName();
  private static final String STAGE_NAME = PipeliteIdCreator.stageName();
  private static final int IMMEDIATE_RETRIES = 0;
  private static final int MAXIMUM_RETRIES = 0;
  private static final List<Integer> NO_PERMANENT_ERRORS = Collections.emptyList();
  private static final int SUCCESS_EXIT_CODE = 0;

  @SpyBean LsfDescribeJobsCache lsfDescribeJobsCache;
  @SpyBean KubernetesDescribeJobsCache kubernetesDescribeJobsCache;

  @Autowired ProcessRunnerPoolManager processRunnerPoolManager;
  @Autowired PipeliteServices pipeliteServices;
  @Autowired PipeliteMetrics pipeliteMetrics;
  @Autowired ServiceConfiguration serviceConfiguration;

  @Profile("PipelineRunnerDescribeJobsGetResultExceptionTest")
  @TestConfiguration
  static class TestConfig {

    @Autowired LsfTestConfiguration lsfTestConfiguration;

    @Autowired KubernetesTestConfiguration kubernetesTestConfiguration;

    @Bean
    public SingleStageSimpleLsfPipeline singleStageSimpleLsfPipeline() {
      return new SingleStageSimpleLsfPipeline(lsfTestConfiguration);
    }

    @Bean
    public SingleStageKubernetesPipeline singleStageKubernetesPipeline() {
      return new SingleStageKubernetesPipeline(kubernetesTestConfiguration);
    }
  }

  public static class SingleStageSimpleLsfPipeline
      extends ConfigurableTestPipeline<TestProcessConfiguration> {
    public SingleStageSimpleLsfPipeline(LsfTestConfiguration lsfTestConfiguration) {
      super(
          PARALLELISM,
          PROCESS_CNT,
          new TestProcessConfiguration(PIPELINE_NAME_SIMPLE_LSF) {
            @Override
            public void configureProcess(ProcessBuilder builder) {
              SimpleLsfExecutorParameters params =
                  ExecutorTestParameters.simpleLsfParams(
                      lsfTestConfiguration,
                      IMMEDIATE_RETRIES,
                      MAXIMUM_RETRIES,
                      NO_PERMANENT_ERRORS);
              ExecutorTestExitCode.withSimpleLsfExecutor(
                  builder.execute(STAGE_NAME), SUCCESS_EXIT_CODE, params);
            }
          });
    }
  }

  public static class SingleStageKubernetesPipeline
      extends ConfigurableTestPipeline<TestProcessConfiguration> {
    public SingleStageKubernetesPipeline(KubernetesTestConfiguration kubernetesTestConfiguration) {
      super(
          PARALLELISM,
          PROCESS_CNT,
          new TestProcessConfiguration(PIPELINE_NAME_KUBERNETES) {
            @Override
            public void configureProcess(ProcessBuilder builder) {
              KubernetesExecutorParameters params =
                  ExecutorTestParameters.kubernetesParams(
                      kubernetesTestConfiguration,
                      IMMEDIATE_RETRIES,
                      MAXIMUM_RETRIES,
                      NO_PERMANENT_ERRORS);
              ExecutorTestExitCode.withKubernetesExecutor(
                  builder.execute(STAGE_NAME), SUCCESS_EXIT_CODE, params);
            }
          });
    }
  }

  @Test
  public void success() {

    processRunnerPoolManager.createPools();
    processRunnerPoolManager.startPools();
    processRunnerPoolManager.waitPoolsToStop();

    List<String> pipelineNames = Arrays.asList(PIPELINE_NAME_SIMPLE_LSF, PIPELINE_NAME_KUBERNETES);
    for (String pipelineName : pipelineNames) {
      RunnerService runnerService = pipeliteServices.runner();
      assertThat(
              runnerService
                  .getPipelineRunner(pipelineName)
                  .get()
                  .getActiveProcessRunners()
                  .isEmpty())
          .isTrue();

      ProcessMetrics processMetrics = pipeliteMetrics.process(pipelineName);
      assertThat(processMetrics.runner().completedCount()).isEqualTo(PROCESS_CNT);
      assertThat(processMetrics.runner().failedCount()).isZero();
      assertThat(processMetrics.stage(STAGE_NAME).runner().failedCount()).isEqualTo(0);
      assertThat(processMetrics.stage(STAGE_NAME).runner().successCount()).isEqualTo(PROCESS_CNT);
    }
  }

  // The stage execution is expected to fail if DescribeJobs.getResult() throws an exception.
  @Test
  public void getDescribeJobsResultThrowsException() {

    AtomicInteger lsfExceptionCount = new AtomicInteger();
    AtomicInteger kubernetesExceptionCount = new AtomicInteger();

    Answer<DescribeJobs<LsfRequestContext, LsfExecutorContext>> createLsfDescribeJobsAnswer =
        invocation -> {
          AbstractLsfExecutor<AbstractLsfExecutorParameters> executor =
              invocation.getArgument(0, AbstractLsfExecutor.class);

          DescribeJobs<LsfRequestContext, LsfExecutorContext> lsfDescribeJobs =
              spy(
                  new DescribeJobs<>(
                      serviceConfiguration,
                      pipeliteServices.internalError(),
                      lsfDescribeJobsCache.requestLimit(),
                      lsfDescribeJobsCache.getExecutorContext(executor)));
          doAnswer(
                  invocation2 -> {
                    lsfExceptionCount.incrementAndGet();
                    throw new PipeliteException("Expected exception from getResult");
                  })
              .when(lsfDescribeJobs)
              .getResult(any(), any());

          return lsfDescribeJobs;
        };
    doAnswer(createLsfDescribeJobsAnswer).when(lsfDescribeJobsCache).createDescribeJobs(any());

    Answer<DescribeJobs<DefaultRequestContext, KubernetesExecutorContext>>
        createKubernetesDescribeJobsAnswer =
            invocation -> {
              KubernetesExecutor executor = invocation.getArgument(0, KubernetesExecutor.class);

              DescribeJobs<DefaultRequestContext, KubernetesExecutorContext> lsfDescribeJobs =
                  spy(
                      new DescribeJobs<>(
                          serviceConfiguration,
                          pipeliteServices.internalError(),
                          kubernetesDescribeJobsCache.requestLimit(),
                          kubernetesDescribeJobsCache.getExecutorContext(executor)));
              doAnswer(
                      invocation2 -> {
                        kubernetesExceptionCount.incrementAndGet();
                        throw new PipeliteException("Expected exception from getResult");
                      })
                  .when(lsfDescribeJobs)
                  .getResult(any(), any());

              return lsfDescribeJobs;
            };
    doAnswer(createKubernetesDescribeJobsAnswer)
        .when(kubernetesDescribeJobsCache)
        .createDescribeJobs(any());

    processRunnerPoolManager.createPools();
    processRunnerPoolManager.startPools();
    processRunnerPoolManager.waitPoolsToStop();

    assertThat(lsfExceptionCount.get()).isEqualTo(PROCESS_CNT);
    assertThat(kubernetesExceptionCount.get()).isEqualTo(PROCESS_CNT);

    List<String> pipelineNames = Arrays.asList(PIPELINE_NAME_SIMPLE_LSF, PIPELINE_NAME_KUBERNETES);
    for (String pipelineName : pipelineNames) {
      RunnerService runnerService = pipeliteServices.runner();
      assertThat(
              runnerService
                  .getPipelineRunner(pipelineName)
                  .get()
                  .getActiveProcessRunners()
                  .isEmpty())
          .isTrue();

      ProcessMetrics processMetrics = pipeliteMetrics.process(pipelineName);
      assertThat(processMetrics.runner().failedCount()).isEqualTo(PROCESS_CNT);
      assertThat(processMetrics.runner().completedCount()).isZero();
      assertThat(processMetrics.stage(STAGE_NAME).runner().successCount()).isEqualTo(0);
      assertThat(processMetrics.stage(STAGE_NAME).runner().failedCount()).isEqualTo(PROCESS_CNT);
      assertThat(pipeliteMetrics.error().count()).isEqualTo(PROCESS_CNT * pipelineNames.size());
    }
  }

  // The stage execution is expected to continue if an exception is thrown
  // in DescribeJobs.retrieveResults(). In this test we will throw the
  // exception from ExecutorContext.pollJobs().
  @Test
  public void getDescribeJobsRetrieveResultsThrowsException() {

    AtomicInteger lsfExceptionCount = new AtomicInteger();
    AtomicInteger kubernetesExceptionCount = new AtomicInteger();

    Answer<DescribeJobs<LsfRequestContext, LsfExecutorContext>> createLsfDescribeJobsAnswer =
        invocation -> {
          AbstractLsfExecutor<AbstractLsfExecutorParameters> executor =
              invocation.getArgument(0, AbstractLsfExecutor.class);

          AtomicBoolean throwException = new AtomicBoolean(true);
          LsfExecutorContext executorContext =
              spy(lsfDescribeJobsCache.getExecutorContext(executor));
          doAnswer(
                  invocation2 -> {
                    if (throwException.get()) {
                      throwException.set(false);
                      lsfExceptionCount.incrementAndGet();
                      // Throw exception once.
                      throw new PipeliteException("Expected exception from pollJobs");
                    }
                    return invocation2.callRealMethod();
                  })
              .when(executorContext)
              .pollJobs(any());
          return spy(
              new DescribeJobs<>(
                  serviceConfiguration,
                  pipeliteServices.internalError(),
                  lsfDescribeJobsCache.requestLimit(),
                  executorContext));
        };
    doAnswer(createLsfDescribeJobsAnswer).when(lsfDescribeJobsCache).createDescribeJobs(any());

    Answer<DescribeJobs<DefaultRequestContext, KubernetesExecutorContext>>
        createKubernetesDescribeJobsAnswer =
            invocation -> {
              KubernetesExecutor executor = invocation.getArgument(0, KubernetesExecutor.class);

              AtomicBoolean throwException = new AtomicBoolean(true);
              KubernetesExecutorContext executorContext =
                  spy(kubernetesDescribeJobsCache.getExecutorContext(executor));
              doAnswer(
                      invocation2 -> {
                        if (throwException.get()) {
                          throwException.set(false);
                          kubernetesExceptionCount.incrementAndGet();
                          // Throw exception once.
                          throw new PipeliteException("Expected exception from pollJobs");
                        }
                        return invocation2.callRealMethod();
                      })
                  .when(executorContext)
                  .pollJobs(any());
              return spy(
                  new DescribeJobs<>(
                      serviceConfiguration,
                      pipeliteServices.internalError(),
                      kubernetesDescribeJobsCache.requestLimit(),
                      executorContext));
            };
    doAnswer(createKubernetesDescribeJobsAnswer)
        .when(kubernetesDescribeJobsCache)
        .createDescribeJobs(any());

    processRunnerPoolManager.createPools();
    processRunnerPoolManager.startPools();
    processRunnerPoolManager.waitPoolsToStop();

    assertThat(lsfExceptionCount.get()).isEqualTo(1);
    assertThat(kubernetesExceptionCount.get()).isEqualTo(1);

    List<String> pipelineNames = Arrays.asList(PIPELINE_NAME_SIMPLE_LSF, PIPELINE_NAME_KUBERNETES);
    for (String pipelineName : pipelineNames) {
      RunnerService runnerService = pipeliteServices.runner();
      assertThat(
              runnerService
                  .getPipelineRunner(pipelineName)
                  .get()
                  .getActiveProcessRunners()
                  .isEmpty())
          .isTrue();

      ProcessMetrics processMetrics = pipeliteMetrics.process(pipelineName);
      assertThat(processMetrics.runner().completedCount()).isEqualTo(PROCESS_CNT);
      assertThat(processMetrics.runner().failedCount()).isZero();
      assertThat(processMetrics.stage(STAGE_NAME).runner().failedCount()).isEqualTo(0);
      assertThat(processMetrics.stage(STAGE_NAME).runner().successCount()).isEqualTo(PROCESS_CNT);
      assertThat(pipeliteMetrics.error().count()).isEqualTo(PROCESS_CNT);
    }
  }
}
