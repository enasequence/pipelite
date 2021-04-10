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
import lombok.Value;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Profile;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import pipelite.PipeliteTestConfigWithManager;
import pipelite.PrioritizedPipelineTestHelper;
import pipelite.executor.AbstractExecutor;
import pipelite.manager.ProcessRunnerPoolManager;
import pipelite.metrics.PipelineMetrics;
import pipelite.metrics.PipeliteMetrics;
import pipelite.process.builder.ProcessBuilder;
import pipelite.service.PipeliteServices;
import pipelite.stage.executor.StageExecutor;
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

  @Autowired private ProcessRunnerPoolManager processRunnerPoolManager;
  @Autowired private PipeliteServices pipeliteServices;
  @Autowired private PipeliteMetrics metrics;
  @Autowired private TestPipeline<SubmitSuccessPollSuccessExecutor> submitSuccessPollSuccess;

  @Autowired private TestPipeline<SubmitErrorExecutor> submitError;
  @Autowired private TestPipeline<SubmitExceptionExecutor> submitException;
  @Autowired private TestPipeline<PollErrorExecutor> pollError;
  @Autowired private TestPipeline<PollExceptionExecutor> pollException;

  @Profile("PipelineRunnerAsyncTest")
  @TestConfiguration
  static class TestConfig {
    @Bean
    public TestPipeline<SubmitSuccessPollSuccessExecutor> submitSuccessPollSuccess() {
      return new TestPipeline<>(new SubmitSuccessPollSuccessExecutor());
    }

    @Bean
    public TestPipeline<SubmitErrorExecutor> submitError() {
      return new TestPipeline<>(new SubmitErrorExecutor());
    }

    @Bean
    public TestPipeline<SubmitExceptionExecutor> submitException() {
      return new TestPipeline<>(new SubmitExceptionExecutor());
    }

    @Bean
    public TestPipeline<PollErrorExecutor> pollError() {
      return new TestPipeline<>(new PollErrorExecutor());
    }

    @Bean
    public TestPipeline<PollExceptionExecutor> pollException() {
      return new TestPipeline<>(new PollExceptionExecutor());
    }
  }

  @Value
  public static class TestPipeline<T extends StageExecutor> extends PrioritizedPipelineTestHelper {
    private final T stageExecutor;

    public TestPipeline(T stageExecutor) {
      super(2, PROCESS_CNT);
      this.stageExecutor = stageExecutor;
    }

    @Override
    public void _configureProcess(ProcessBuilder builder) {
      ExecutorParameters executorParams =
          ExecutorParameters.builder().immediateRetries(0).maximumRetries(0).build();
      builder.execute("STAGE").with(stageExecutor, executorParams);
    }
  }

  public abstract static class TestExecutor extends AbstractExecutor<ExecutorParameters> {
    public final AtomicInteger submitCount = new AtomicInteger();
    public final AtomicInteger pollCount = new AtomicInteger();
    private final Set<String> submit = ConcurrentHashMap.newKeySet();

    protected boolean isSubmitted(String processId) {
      if (!submit.contains(processId)) {
        submit.add(processId);
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
      if (!isSubmitted(request.getProcessId())) {
        submitCount.incrementAndGet();
        return StageExecutorResult.active();
      } else {
        pollCount.incrementAndGet();
        return StageExecutorResult.success();
      }
    }
  }

  public static class SubmitErrorExecutor extends TestExecutor {
    @Override
    public StageExecutorResult execute(StageExecutorRequest request) {
      if (!isSubmitted(request.getProcessId())) {
        submitCount.incrementAndGet();
        return StageExecutorResult.error();
      } else {
        pollCount.incrementAndGet();
        throw new RuntimeException("Unexpected call to execute");
      }
    }
  }

  public static class SubmitExceptionExecutor extends TestExecutor {
    @Override
    public StageExecutorResult execute(StageExecutorRequest request) {
      if (!isSubmitted(request.getProcessId())) {
        submitCount.incrementAndGet();
        throw new RuntimeException("Expected exception from submit");
      } else {
        pollCount.incrementAndGet();
        throw new RuntimeException("Unexpected call to execute");
      }
    }
  }

  public static class PollErrorExecutor extends TestExecutor {
    @Override
    public StageExecutorResult execute(StageExecutorRequest request) {
      if (!isSubmitted(request.getProcessId())) {
        submitCount.incrementAndGet();
        return StageExecutorResult.active();
      } else {
        pollCount.incrementAndGet();
        return StageExecutorResult.error();
      }
    }
  }

  public static class PollExceptionExecutor extends TestExecutor {
    @Override
    public StageExecutorResult execute(StageExecutorRequest request) {
      if (!isSubmitted(request.getProcessId())) {
        submitCount.incrementAndGet();
        return StageExecutorResult.active();
      } else {
        pollCount.incrementAndGet();
        throw new RuntimeException("Expected exception from poll");
      }
    }
  }

  private void assertSubmitSuccessPollSuccess() {
    TestPipeline<SubmitSuccessPollSuccessExecutor> f = submitSuccessPollSuccess;

    PipelineRunner pipelineRunner =
        pipeliteServices.runner().getPipelineRunner(f.pipelineName()).get();

    assertThat(pipelineRunner.getActiveProcessRunners().size()).isEqualTo(0);

    PipelineMetrics pipelineMetrics = metrics.pipeline(f.pipelineName());
    assertThat(pipelineMetrics.process().getInternalErrorCount()).isEqualTo(0);
    assertThat(pipelineMetrics.process().getCompletedCount()).isEqualTo(PROCESS_CNT);
    assertThat(pipelineMetrics.process().getFailedCount()).isZero();
    assertThat(pipelineMetrics.stage().getFailedCount()).isEqualTo(0);
    assertThat(pipelineMetrics.stage().getSuccessCount()).isEqualTo(PROCESS_CNT);

    assertThat(f.stageExecutor.submitCount.get()).isEqualTo(PROCESS_CNT);
    assertThat(f.stageExecutor.pollCount.get()).isEqualTo(PROCESS_CNT);
  }

  private void assertSubmitError() {
    TestPipeline<SubmitErrorExecutor> f = submitError;

    PipelineRunner pipelineRunner =
        pipeliteServices.runner().getPipelineRunner(f.pipelineName()).get();

    assertThat(pipelineRunner.getActiveProcessRunners().size()).isEqualTo(0);

    PipelineMetrics pipelineMetrics = metrics.pipeline(f.pipelineName());

    assertThat(pipelineMetrics.process().getInternalErrorCount()).isEqualTo(0);
    assertThat(pipelineMetrics.process().getCompletedCount()).isZero();
    assertThat(pipelineMetrics.process().getFailedCount()).isEqualTo(PROCESS_CNT);
    assertThat(pipelineMetrics.stage().getFailedCount()).isEqualTo(PROCESS_CNT);
    assertThat(pipelineMetrics.stage().getSuccessCount()).isEqualTo(0);

    assertThat(f.stageExecutor.submitCount.get()).isEqualTo(PROCESS_CNT);
    assertThat(f.stageExecutor.pollCount.get()).isEqualTo(0);
  }

  private void assertSubmitException() {
    TestPipeline<SubmitExceptionExecutor> f = submitException;

    PipelineRunner pipelineRunner =
        pipeliteServices.runner().getPipelineRunner(f.pipelineName()).get();

    assertThat(pipelineRunner.getActiveProcessRunners().size()).isEqualTo(0);

    PipelineMetrics pipelineMetrics = metrics.pipeline(f.pipelineName());
    assertThat(pipelineMetrics.process().getInternalErrorCount()).isEqualTo(PROCESS_CNT);
    assertThat(pipelineMetrics.process().getCompletedCount()).isZero();
    assertThat(pipelineMetrics.process().getFailedCount()).isEqualTo(PROCESS_CNT);
    assertThat(pipelineMetrics.stage().getFailedCount()).isEqualTo(PROCESS_CNT);
    assertThat(pipelineMetrics.stage().getSuccessCount()).isEqualTo(0);

    assertThat(f.stageExecutor.submitCount.get()).isEqualTo(PROCESS_CNT);
    assertThat(f.stageExecutor.pollCount.get()).isEqualTo(0);
  }

  private void assertPollError() {
    TestPipeline<PollErrorExecutor> f = pollError;

    PipelineRunner pipelineRunner =
        pipeliteServices.runner().getPipelineRunner(f.pipelineName()).get();

    assertThat(pipelineRunner.getActiveProcessRunners().size()).isEqualTo(0);

    PipelineMetrics pipelineMetrics = metrics.pipeline(f.pipelineName());
    assertThat(pipelineMetrics.process().getInternalErrorCount()).isEqualTo(0);
    assertThat(pipelineMetrics.process().getCompletedCount()).isZero();
    assertThat(pipelineMetrics.process().getFailedCount()).isEqualTo(PROCESS_CNT);
    assertThat(pipelineMetrics.stage().getFailedCount()).isEqualTo(PROCESS_CNT);
    assertThat(pipelineMetrics.stage().getSuccessCount()).isEqualTo(0);

    assertThat(f.stageExecutor.submitCount.get()).isEqualTo(PROCESS_CNT);
    assertThat(f.stageExecutor.pollCount.get()).isEqualTo(PROCESS_CNT);
  }

  private void assertPollException() {
    TestPipeline<PollExceptionExecutor> f = pollException;

    PipelineRunner pipelineRunner =
        pipeliteServices.runner().getPipelineRunner(f.pipelineName()).get();

    assertThat(pipelineRunner.getActiveProcessRunners().size()).isEqualTo(0);

    PipelineMetrics pipelineMetrics = metrics.pipeline(f.pipelineName());
    assertThat(pipelineMetrics.process().getInternalErrorCount()).isEqualTo(PROCESS_CNT);
    assertThat(pipelineMetrics.process().getCompletedCount()).isZero();
    assertThat(pipelineMetrics.process().getFailedCount()).isEqualTo(PROCESS_CNT);
    assertThat(pipelineMetrics.stage().getFailedCount()).isEqualTo(PROCESS_CNT);
    assertThat(pipelineMetrics.stage().getSuccessCount()).isEqualTo(0);

    assertThat(f.stageExecutor.submitCount.get()).isEqualTo(PROCESS_CNT);
    assertThat(f.stageExecutor.pollCount.get()).isEqualTo(PROCESS_CNT);
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
