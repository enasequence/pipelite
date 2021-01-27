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
package pipelite.launcher;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.Value;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import pipelite.Pipeline;
import pipelite.PipeliteTestConfiguration;
import pipelite.ProcessSource;
import pipelite.TestProcessSource;
import pipelite.UniqueStringGenerator;
import pipelite.configuration.AdvancedConfiguration;
import pipelite.configuration.ExecutorConfiguration;
import pipelite.configuration.ServiceConfiguration;
import pipelite.executor.AbstractExecutor;
import pipelite.metrics.PipelineMetrics;
import pipelite.metrics.PipeliteMetrics;
import pipelite.process.Process;
import pipelite.process.builder.ProcessBuilder;
import pipelite.service.*;
import pipelite.stage.executor.StageExecutor;
import pipelite.stage.executor.StageExecutorRequest;
import pipelite.stage.executor.StageExecutorResult;
import pipelite.stage.parameters.ExecutorParameters;

@SpringBootTest(
    webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
    classes = PipeliteTestConfiguration.class,
    properties = {
      "pipelite.advanced.processRunnerFrequency=250ms",
      "pipelite.advanced.shutdownIfIdle=true"
    })
@ContextConfiguration(initializers = PipeliteTestConfiguration.TestContextInitializer.class)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
public class PipeliteLauncherAsyncTest {

  private static final int PROCESS_CNT = 2;

  @Autowired private ServiceConfiguration serviceConfiguration;
  @Autowired private AdvancedConfiguration advancedConfiguration;
  @Autowired private ExecutorConfiguration executorConfiguration;
  @Autowired private RegisteredPipelineService registeredPipelineService;
  @Autowired private RegisteredProcessSourceService registeredProcessSourceService;
  @Autowired private ProcessService processService;
  @Autowired private StageService stageService;
  @Autowired private PipeliteLockerService pipeliteLockerService;
  @Autowired private MailService mailService;
  @Autowired private PipeliteMetrics metrics;

  @Autowired private TestPipeline<SubmitSuccessPollSuccessExecutor> submitSuccessPollSuccess;

  @Autowired public TestPipeline<SubmitErrorExecutor> submitError;
  @Autowired public TestPipeline<SubmitExceptionExecutor> submitException;
  @Autowired public TestPipeline<PollErrorExecutor> pollError;
  @Autowired public TestPipeline<PollExceptionExecutor> pollException;

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

    @Bean
    public ProcessSource submitSuccessPollSuccessSource(
        @Autowired TestPipeline<SubmitSuccessPollSuccessExecutor> f) {
      return new TestProcessSource(f.getPipelineName(), PROCESS_CNT);
    }

    @Bean
    public ProcessSource submitErrorSource(@Autowired TestPipeline<SubmitErrorExecutor> f) {
      return new TestProcessSource(f.getPipelineName(), PROCESS_CNT);
    }

    @Bean
    public ProcessSource submitExceptionSource(@Autowired TestPipeline<SubmitExceptionExecutor> f) {
      return new TestProcessSource(f.getPipelineName(), PROCESS_CNT);
    }

    @Bean
    public ProcessSource pollErrorSource(@Autowired TestPipeline<PollErrorExecutor> f) {
      return new TestProcessSource(f.getPipelineName(), PROCESS_CNT);
    }

    @Bean
    public ProcessSource pollExceptionSource(@Autowired TestPipeline<PollExceptionExecutor> f) {
      return new TestProcessSource(f.getPipelineName(), PROCESS_CNT);
    }
  }

  private PipeliteLauncher createPipeliteLauncher(String pipelineName) {
    return DefaultPipeliteLauncher.create(
        serviceConfiguration,
        advancedConfiguration,
        executorConfiguration,
        pipeliteLockerService.getPipeliteLocker(),
        registeredPipelineService,
        registeredProcessSourceService,
        processService,
        stageService,
        mailService,
        metrics,
        pipelineName);
  }

  @Value
  public static class TestPipeline<T extends StageExecutor> implements Pipeline {
    private final String pipelineName = UniqueStringGenerator.randomPipelineName();
    private final T stageExecutor;
    public final List<String> processIds = Collections.synchronizedList(new ArrayList<>());

    public TestPipeline(T stageExecutor) {
      this.stageExecutor = stageExecutor;
    }

    public void reset() {
      processIds.clear();
    }

    @Override
    public String getPipelineName() {
      return pipelineName;
    }

    @Override
    public int getPipelineParallelism() {
      return 2;
    }

    @Override
    public Process createProcess(ProcessBuilder builder) {
      processIds.add(builder.getProcessId());
      ExecutorParameters executorParams =
          ExecutorParameters.builder().immediateRetries(0).maximumRetries(0).build();
      return builder.execute("STAGE").with(stageExecutor, executorParams).build();
    }
  }

  public abstract static class TestExecutor extends AbstractExecutor<ExecutorParameters> {
    public AtomicInteger submitCount = new AtomicInteger();
    public AtomicInteger pollCount = new AtomicInteger();
    private Set<String> submit = ConcurrentHashMap.newKeySet();

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

  @Test
  public void testSubmitSuccessPollSuccess() {
    TestPipeline<SubmitSuccessPollSuccessExecutor> f = submitSuccessPollSuccess;

    PipeliteLauncher pipeliteLauncher = createPipeliteLauncher(f.getPipelineName());
    new PipeliteServiceManager().addService(pipeliteLauncher).runSync();

    PipelineMetrics pipelineMetrics = metrics.pipeline(f.getPipelineName());
    assertThat(pipelineMetrics.getInternalErrorCount()).isEqualTo(0);
    assertThat(pipelineMetrics.process().getCompletedCount()).isEqualTo(PROCESS_CNT);
    assertThat(pipelineMetrics.process().getFailedCount()).isZero();
    assertThat(pipelineMetrics.stage().getFailedCount()).isEqualTo(0);
    assertThat(pipelineMetrics.stage().getSuccessCount()).isEqualTo(PROCESS_CNT);

    assertThat(f.stageExecutor.submitCount.get()).isEqualTo(PROCESS_CNT);
    assertThat(f.stageExecutor.pollCount.get()).isEqualTo(PROCESS_CNT);
  }

  @Test
  public void testSubmitError() {
    TestPipeline<SubmitErrorExecutor> f = submitError;

    PipeliteLauncher pipeliteLauncher = createPipeliteLauncher(f.getPipelineName());
    new PipeliteServiceManager().addService(pipeliteLauncher).runSync();

    PipelineMetrics pipelineMetrics = metrics.pipeline(f.getPipelineName());

    assertThat(pipelineMetrics.getInternalErrorCount()).isEqualTo(0);
    assertThat(pipelineMetrics.process().getCompletedCount()).isZero();
    assertThat(pipelineMetrics.process().getFailedCount()).isEqualTo(PROCESS_CNT);
    assertThat(pipelineMetrics.stage().getFailedCount()).isEqualTo(PROCESS_CNT);
    assertThat(pipelineMetrics.stage().getSuccessCount()).isEqualTo(0);

    assertThat(f.stageExecutor.submitCount.get()).isEqualTo(PROCESS_CNT);
    assertThat(f.stageExecutor.pollCount.get()).isEqualTo(0);
  }

  @Test
  public void testSubmitException() {
    TestPipeline<SubmitExceptionExecutor> f = submitException;

    PipeliteLauncher pipeliteLauncher = createPipeliteLauncher(f.getPipelineName());
    new PipeliteServiceManager().addService(pipeliteLauncher).runSync();

    PipelineMetrics pipelineMetrics = metrics.pipeline(f.getPipelineName());
    assertThat(pipelineMetrics.getInternalErrorCount()).isEqualTo(PROCESS_CNT);
    assertThat(pipelineMetrics.process().getCompletedCount()).isZero();
    assertThat(pipelineMetrics.process().getFailedCount()).isEqualTo(PROCESS_CNT);
    assertThat(pipelineMetrics.stage().getFailedCount()).isEqualTo(PROCESS_CNT);
    assertThat(pipelineMetrics.stage().getSuccessCount()).isEqualTo(0);

    assertThat(f.stageExecutor.submitCount.get()).isEqualTo(PROCESS_CNT);
    assertThat(f.stageExecutor.pollCount.get()).isEqualTo(0);
  }

  @Test
  public void testPollError() {
    TestPipeline<PollErrorExecutor> f = pollError;

    PipeliteLauncher pipeliteLauncher = createPipeliteLauncher(f.getPipelineName());
    new PipeliteServiceManager().addService(pipeliteLauncher).runSync();

    PipelineMetrics pipelineMetrics = metrics.pipeline(f.getPipelineName());
    assertThat(pipelineMetrics.getInternalErrorCount()).isEqualTo(0);
    assertThat(pipelineMetrics.process().getCompletedCount()).isZero();
    assertThat(pipelineMetrics.process().getFailedCount()).isEqualTo(PROCESS_CNT);
    assertThat(pipelineMetrics.stage().getFailedCount()).isEqualTo(PROCESS_CNT);
    assertThat(pipelineMetrics.stage().getSuccessCount()).isEqualTo(0);

    assertThat(f.stageExecutor.submitCount.get()).isEqualTo(PROCESS_CNT);
    assertThat(f.stageExecutor.pollCount.get()).isEqualTo(PROCESS_CNT);
  }

  @Test
  public void testPollException() {
    TestPipeline<PollExceptionExecutor> f = pollException;

    PipeliteLauncher pipeliteLauncher = createPipeliteLauncher(f.getPipelineName());
    new PipeliteServiceManager().addService(pipeliteLauncher).runSync();

    PipelineMetrics pipelineMetrics = metrics.pipeline(f.getPipelineName());
    assertThat(pipelineMetrics.getInternalErrorCount()).isEqualTo(PROCESS_CNT);
    assertThat(pipelineMetrics.process().getCompletedCount()).isZero();
    assertThat(pipelineMetrics.process().getFailedCount()).isEqualTo(PROCESS_CNT);
    assertThat(pipelineMetrics.stage().getFailedCount()).isEqualTo(PROCESS_CNT);
    assertThat(pipelineMetrics.stage().getSuccessCount()).isEqualTo(0);

    assertThat(f.stageExecutor.submitCount.get()).isEqualTo(PROCESS_CNT);
    assertThat(f.stageExecutor.pollCount.get()).isEqualTo(PROCESS_CNT);
  }
}
