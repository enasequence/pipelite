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

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.Getter;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Profile;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import pipelite.PipeliteTestConfigWithManager;
import pipelite.executor.AbstractExecutor;
import pipelite.helper.ConfigurableTestPipeline;
import pipelite.helper.process.TestProcessConfiguration;
import pipelite.manager.ProcessRunnerPoolManager;
import pipelite.metrics.PipelineMetrics;
import pipelite.metrics.PipeliteMetrics;
import pipelite.process.builder.ProcessBuilder;
import pipelite.service.PipeliteServices;
import pipelite.stage.executor.StageExecutorRequest;
import pipelite.stage.executor.StageExecutorResult;
import pipelite.stage.parameters.ExecutorParameters;

@SpringBootTest(
    classes = PipeliteTestConfigWithManager.class,
    properties = {
      "pipelite.service.force=true",
      "pipelite.service.name=PipelineRunnerAsyncTest",
      "pipelite.advanced.processRunnerFrequency=250ms",
      "pipelite.advanced.shutdownIfIdle=true"
    })
@ActiveProfiles({"test", "PipelineRunnerAsyncTest"})
@DirtiesContext
public class PipelineRunnerAsyncTest {

  private static final int PROCESS_CNT = 2;
  private static final int PARALLELISM = 2;

  @Autowired private ProcessRunnerPoolManager processRunnerPoolManager;
  @Autowired private PipeliteServices pipeliteServices;
  @Autowired private PipeliteMetrics metrics;
  @Autowired private TestPipeline submitSuccessPollSuccess;

  @Autowired private TestPipeline submitError;
  @Autowired private TestPipeline submitException;
  @Autowired private TestPipeline pollError;
  @Autowired private TestPipeline pollException;

  @Profile("PipelineRunnerAsyncTest")
  @TestConfiguration
  static class TestConfig {
    @Bean
    public TestPipeline submitSuccessPollSuccess() {
      return new TestPipeline(new SubmitSuccessPollSuccessExecutor());
    }

    @Bean
    public TestPipeline submitError() {
      return new TestPipeline(new SubmitErrorExecutor());
    }

    @Bean
    public TestPipeline submitException() {
      return new TestPipeline(new SubmitExceptionExecutor());
    }

    @Bean
    public TestPipeline pollError() {
      return new TestPipeline(new PollErrorExecutor());
    }

    @Bean
    public TestPipeline pollException() {
      return new TestPipeline(new PollExceptionExecutor());
    }
  }

  @Getter
  public static class TestPipeline<T extends TestExecutor>
      extends ConfigurableTestPipeline<TestProcessConfiguration> {
    private final T stageExecutor;

    public TestPipeline(T stageExecutor) {
      super(
          PARALLELISM,
          PROCESS_CNT,
          new TestProcessConfiguration() {
            @Override
            protected void testConfigureProcess(ProcessBuilder builder) {
              ExecutorParameters executorParams =
                  ExecutorParameters.builder().immediateRetries(0).maximumRetries(0).build();
              builder.execute("STAGE").with(stageExecutor, executorParams);
            }
          });
      this.stageExecutor = stageExecutor;
    }
  }

  public abstract static class TestExecutor extends AbstractExecutor<ExecutorParameters> {
    public final AtomicInteger firstExecuteCalledCount = new AtomicInteger();
    public final AtomicInteger subsequentExecuteCalledCount = new AtomicInteger();
    private final Set<String> executeCalled = ConcurrentHashMap.newKeySet();

    protected boolean isExecuteCalled(String processId) {
      if (!executeCalled.contains(processId)) {
        executeCalled.add(processId);
        return false;
      }
      return true;
    }

    @Override
    public void terminate() {}
  }

  public static class SubmitSuccessPollSuccessExecutor extends TestExecutor {
    @Override
    public StageExecutorResult execute(StageExecutorRequest request) {
      if (!isExecuteCalled(request.getProcessId())) {
        firstExecuteCalledCount.incrementAndGet();
        return StageExecutorResult.active();
      } else {
        subsequentExecuteCalledCount.incrementAndGet();
        return StageExecutorResult.success();
      }
    }
  }

  public static class SubmitErrorExecutor extends TestExecutor {
    @Override
    public StageExecutorResult execute(StageExecutorRequest request) {
      if (!isExecuteCalled(request.getProcessId())) {
        firstExecuteCalledCount.incrementAndGet();
        return StageExecutorResult.error();
      } else {
        subsequentExecuteCalledCount.incrementAndGet();
        throw new RuntimeException("Unexpected call to execute");
      }
    }
  }

  public static class SubmitExceptionExecutor extends TestExecutor {
    @Override
    public StageExecutorResult execute(StageExecutorRequest request) {
      if (!isExecuteCalled(request.getProcessId())) {
        firstExecuteCalledCount.incrementAndGet();
        throw new RuntimeException("Expected exception from submit");
      } else {
        subsequentExecuteCalledCount.incrementAndGet();
        throw new RuntimeException("Unexpected call to execute");
      }
    }
  }

  public static class PollErrorExecutor extends TestExecutor {
    @Override
    public StageExecutorResult execute(StageExecutorRequest request) {
      if (!isExecuteCalled(request.getProcessId())) {
        firstExecuteCalledCount.incrementAndGet();
        return StageExecutorResult.active();
      } else {
        subsequentExecuteCalledCount.incrementAndGet();
        return StageExecutorResult.error();
      }
    }
  }

  public static class PollExceptionExecutor extends TestExecutor {
    @Override
    public StageExecutorResult execute(StageExecutorRequest request) {
      if (!isExecuteCalled(request.getProcessId())) {
        firstExecuteCalledCount.incrementAndGet();
        return StageExecutorResult.active();
      } else {
        subsequentExecuteCalledCount.incrementAndGet();
        throw new RuntimeException("Expected exception from poll");
      }
    }
  }

  private void assertSubmitSuccessPollSuccess() {
    TestPipeline f = submitSuccessPollSuccess;

    PipelineRunner pipelineRunner =
        pipeliteServices.runner().getPipelineRunner(f.pipelineName()).get();

    assertThat(pipelineRunner.getActiveProcessRunners().size()).isEqualTo(0);

    PipelineMetrics pipelineMetrics = metrics.pipeline(f.pipelineName());
    assertThat(pipelineMetrics.process().getInternalErrorCount()).isEqualTo(0);
    assertThat(pipelineMetrics.process().getCompletedCount()).isEqualTo(PROCESS_CNT);
    assertThat(pipelineMetrics.process().getFailedCount()).isZero();
    assertThat(pipelineMetrics.stage().getFailedCount()).isEqualTo(0);
    assertThat(pipelineMetrics.stage().getSuccessCount()).isEqualTo(PROCESS_CNT);

    assertThat(f.stageExecutor.firstExecuteCalledCount.get()).isEqualTo(PROCESS_CNT);
    assertThat(f.stageExecutor.subsequentExecuteCalledCount.get()).isEqualTo(PROCESS_CNT);
  }

  private void assertSubmitError() {
    TestPipeline f = submitError;

    PipelineRunner pipelineRunner =
        pipeliteServices.runner().getPipelineRunner(f.pipelineName()).get();

    assertThat(pipelineRunner.getActiveProcessRunners().size()).isEqualTo(0);

    PipelineMetrics pipelineMetrics = metrics.pipeline(f.pipelineName());

    assertThat(pipelineMetrics.process().getInternalErrorCount()).isEqualTo(0);
    assertThat(pipelineMetrics.process().getCompletedCount()).isZero();
    assertThat(pipelineMetrics.process().getFailedCount()).isEqualTo(PROCESS_CNT);
    assertThat(pipelineMetrics.stage().getFailedCount()).isEqualTo(PROCESS_CNT);
    assertThat(pipelineMetrics.stage().getSuccessCount()).isEqualTo(0);

    assertThat(f.stageExecutor.firstExecuteCalledCount.get()).isEqualTo(PROCESS_CNT);
    assertThat(f.stageExecutor.subsequentExecuteCalledCount.get()).isEqualTo(0);
  }

  private void assertSubmitException() {
    TestPipeline f = submitException;

    PipelineRunner pipelineRunner =
        pipeliteServices.runner().getPipelineRunner(f.pipelineName()).get();

    assertThat(pipelineRunner.getActiveProcessRunners().size()).isEqualTo(0);

    PipelineMetrics pipelineMetrics = metrics.pipeline(f.pipelineName());
    assertThat(pipelineMetrics.process().getInternalErrorCount()).isEqualTo(PROCESS_CNT);
    assertThat(pipelineMetrics.process().getCompletedCount()).isZero();
    assertThat(pipelineMetrics.process().getFailedCount()).isEqualTo(PROCESS_CNT);
    assertThat(pipelineMetrics.stage().getFailedCount()).isEqualTo(PROCESS_CNT);
    assertThat(pipelineMetrics.stage().getSuccessCount()).isEqualTo(0);

    assertThat(f.stageExecutor.firstExecuteCalledCount.get()).isEqualTo(PROCESS_CNT);
    assertThat(f.stageExecutor.subsequentExecuteCalledCount.get()).isEqualTo(0);
  }

  private void assertPollError() {
    TestPipeline f = pollError;

    PipelineRunner pipelineRunner =
        pipeliteServices.runner().getPipelineRunner(f.pipelineName()).get();

    assertThat(pipelineRunner.getActiveProcessRunners().size()).isEqualTo(0);

    PipelineMetrics pipelineMetrics = metrics.pipeline(f.pipelineName());
    assertThat(pipelineMetrics.process().getInternalErrorCount()).isEqualTo(0);
    assertThat(pipelineMetrics.process().getCompletedCount()).isZero();
    assertThat(pipelineMetrics.process().getFailedCount()).isEqualTo(PROCESS_CNT);
    assertThat(pipelineMetrics.stage().getFailedCount()).isEqualTo(PROCESS_CNT);
    assertThat(pipelineMetrics.stage().getSuccessCount()).isEqualTo(0);

    assertThat(f.stageExecutor.firstExecuteCalledCount.get()).isEqualTo(PROCESS_CNT);
    assertThat(f.stageExecutor.subsequentExecuteCalledCount.get()).isEqualTo(PROCESS_CNT);
  }

  private void assertPollException() {
    TestPipeline f = pollException;

    PipelineRunner pipelineRunner =
        pipeliteServices.runner().getPipelineRunner(f.pipelineName()).get();

    assertThat(pipelineRunner.getActiveProcessRunners().size()).isEqualTo(0);

    PipelineMetrics pipelineMetrics = metrics.pipeline(f.pipelineName());
    assertThat(pipelineMetrics.process().getInternalErrorCount()).isEqualTo(PROCESS_CNT);
    assertThat(pipelineMetrics.process().getCompletedCount()).isZero();
    assertThat(pipelineMetrics.process().getFailedCount()).isEqualTo(PROCESS_CNT);
    assertThat(pipelineMetrics.stage().getFailedCount()).isEqualTo(PROCESS_CNT);
    assertThat(pipelineMetrics.stage().getSuccessCount()).isEqualTo(0);

    assertThat(f.stageExecutor.firstExecuteCalledCount.get()).isEqualTo(PROCESS_CNT);
    assertThat(f.stageExecutor.subsequentExecuteCalledCount.get()).isEqualTo(PROCESS_CNT);
  }

  @Test
  public void testPipelines() {
    processRunnerPoolManager.createPools();
    processRunnerPoolManager.startPools();
    processRunnerPoolManager.waitPoolsToStop();

    assertSubmitSuccessPollSuccess();
    assertSubmitError();
    assertSubmitException();
    assertPollError();
    assertPollException();
  }
}
