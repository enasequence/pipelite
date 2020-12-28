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

import io.micrometer.core.instrument.MeterRegistry;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.Value;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;
import pipelite.TestProcessSource;
import pipelite.UniqueStringGenerator;
import pipelite.configuration.LauncherConfiguration;
import pipelite.configuration.StageConfiguration;
import pipelite.configuration.WebConfiguration;
import pipelite.launcher.process.runner.ProcessRunnerMetrics;
import pipelite.process.Process;
import pipelite.process.ProcessFactory;
import pipelite.process.ProcessSource;
import pipelite.process.builder.ProcessBuilder;
import pipelite.service.*;
import pipelite.stage.Stage;
import pipelite.stage.executor.StageExecutor;
import pipelite.stage.executor.StageExecutorParameters;
import pipelite.stage.executor.StageExecutorResult;

@Component
@Scope("prototype")
public class PipeliteLauncherAsyncTester {

  private static final int PROCESS_CNT = 2;

  @Autowired private WebConfiguration webConfiguration;
  @Autowired private LauncherConfiguration launcherConfiguration;
  @Autowired private StageConfiguration stageConfiguration;
  @Autowired private ProcessFactoryService processFactoryService;
  @Autowired private ProcessSourceService processSourceService;
  @Autowired private ScheduleService scheduleService;
  @Autowired private ProcessService processService;
  @Autowired private StageService stageService;
  @Autowired private LockService lockService;
  @Autowired private MailService mailService;
  @Autowired private MeterRegistry meterRegistry;

  @Autowired private TestProcessFactory<SubmitSuccessPollSuccessExecutor> submitSuccessPollSuccess;
  @Autowired public TestProcessFactory<SubmitErrorExecutor> submitError;
  @Autowired public TestProcessFactory<SubmitExceptionExecutor> submitException;
  @Autowired public TestProcessFactory<PollErrorExecutor> pollError;
  @Autowired public TestProcessFactory<PollExceptionExecutor> pollException;

  @TestConfiguration
  static class TestConfig {
    @Bean
    public TestProcessFactory<SubmitSuccessPollSuccessExecutor> submitSuccessPollSuccess() {
      return new TestProcessFactory<>(new SubmitSuccessPollSuccessExecutor());
    }

    @Bean
    public TestProcessFactory<SubmitErrorExecutor> submitError() {
      return new TestProcessFactory<>(new SubmitErrorExecutor());
    }

    @Bean
    public TestProcessFactory<SubmitExceptionExecutor> submitException() {
      return new TestProcessFactory<>(new SubmitExceptionExecutor());
    }

    @Bean
    public TestProcessFactory<PollErrorExecutor> pollError() {
      return new TestProcessFactory<>(new PollErrorExecutor());
    }

    @Bean
    public TestProcessFactory<PollExceptionExecutor> pollException() {
      return new TestProcessFactory<>(new PollExceptionExecutor());
    }

    @Bean
    public ProcessSource submitSuccessPollSuccessSource(
        @Autowired TestProcessFactory<SubmitSuccessPollSuccessExecutor> f) {
      return new TestProcessSource(f.getPipelineName(), PROCESS_CNT);
    }

    @Bean
    public ProcessSource submitErrorSource(@Autowired TestProcessFactory<SubmitErrorExecutor> f) {
      return new TestProcessSource(f.getPipelineName(), PROCESS_CNT);
    }

    @Bean
    public ProcessSource submitExceptionSource(
        @Autowired TestProcessFactory<SubmitExceptionExecutor> f) {
      return new TestProcessSource(f.getPipelineName(), PROCESS_CNT);
    }

    @Bean
    public ProcessSource pollErrorSource(@Autowired TestProcessFactory<PollErrorExecutor> f) {
      return new TestProcessSource(f.getPipelineName(), PROCESS_CNT);
    }

    @Bean
    public ProcessSource pollExceptionSource(
        @Autowired TestProcessFactory<PollExceptionExecutor> f) {
      return new TestProcessSource(f.getPipelineName(), PROCESS_CNT);
    }
  }

  private PipeliteLauncher createPipeliteLauncher(String pipelineName) {
    return DefaultPipeliteLauncher.create(
        webConfiguration,
        launcherConfiguration,
        stageConfiguration,
        lockService,
        processFactoryService,
        processSourceService,
        processService,
        stageService,
        mailService,
        meterRegistry,
        pipelineName);
  }

  @Value
  public static class TestProcessFactory<T extends StageExecutor> implements ProcessFactory {
    private final String pipelineName = UniqueStringGenerator.randomPipelineName();
    private final T stageExecutor;
    public final List<String> processIds = Collections.synchronizedList(new ArrayList<>());

    public TestProcessFactory(T stageExecutor) {
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
    public int getProcessParallelism() {
      return 2;
    }

    @Override
    public Process create(String processId) {
      processIds.add(processId);
      StageExecutorParameters executorParams =
          StageExecutorParameters.builder().immediateRetries(0).maximumRetries(0).build();
      return new ProcessBuilder(processId)
          .execute("STAGE", executorParams)
          .with(stageExecutor)
          .build();
    }
  }

  public abstract static class TestExecutor implements StageExecutor {
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
  }

  public static class SubmitSuccessPollSuccessExecutor extends TestExecutor {
    @Override
    public StageExecutorResult execute(String pipelineName, String processId, Stage stage) {
      if (!isSubmitted(processId)) {
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
    public StageExecutorResult execute(String pipelineName, String processId, Stage stage) {
      if (!isSubmitted(processId)) {
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
    public StageExecutorResult execute(String pipelineName, String processId, Stage stage) {
      if (!isSubmitted(processId)) {
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
    public StageExecutorResult execute(String pipelineName, String processId, Stage stage) {
      if (!isSubmitted(processId)) {
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
    public StageExecutorResult execute(String pipelineName, String processId, Stage stage) {
      if (!isSubmitted(processId)) {
        submitCount.incrementAndGet();
        return StageExecutorResult.active();
      } else {
        pollCount.incrementAndGet();
        throw new RuntimeException("Expected exception from poll");
      }
    }
  }

  public void testSubmitSuccessPollSuccess() {
    TestProcessFactory<SubmitSuccessPollSuccessExecutor> f = submitSuccessPollSuccess;

    PipeliteLauncher pipeliteLauncher = createPipeliteLauncher(f.getPipelineName());
    new PipeliteServiceManager().addService(pipeliteLauncher).runSync();

    ProcessRunnerMetrics metrics = pipeliteLauncher.getMetrics();
    assertThat(metrics.getInternalErrorCount()).isEqualTo(0);
    assertThat(metrics.getProcessCompletedCount()).isEqualTo(PROCESS_CNT);
    assertThat(metrics.getProcessFailedCount()).isZero();
    assertThat(metrics.getStageFailedCount()).isEqualTo(0);
    assertThat(metrics.getStageSuccessCount()).isEqualTo(PROCESS_CNT);

    assertThat(f.stageExecutor.submitCount.get()).isEqualTo(PROCESS_CNT);
    assertThat(f.stageExecutor.pollCount.get()).isEqualTo(PROCESS_CNT);
  }

  public void testSubmitError() {
    TestProcessFactory<SubmitErrorExecutor> f = submitError;

    PipeliteLauncher pipeliteLauncher = createPipeliteLauncher(f.getPipelineName());
    new PipeliteServiceManager().addService(pipeliteLauncher).runSync();

    ProcessRunnerMetrics metrics = pipeliteLauncher.getMetrics();
    assertThat(metrics.getInternalErrorCount()).isEqualTo(0);
    assertThat(metrics.getProcessCompletedCount()).isZero();
    assertThat(metrics.getProcessFailedCount()).isEqualTo(PROCESS_CNT);
    assertThat(metrics.getStageFailedCount()).isEqualTo(PROCESS_CNT);
    assertThat(metrics.getStageSuccessCount()).isEqualTo(0);

    assertThat(f.stageExecutor.submitCount.get()).isEqualTo(PROCESS_CNT);
    assertThat(f.stageExecutor.pollCount.get()).isEqualTo(0);
  }

  public void testSubmitException() {
    TestProcessFactory<SubmitExceptionExecutor> f = submitException;

    PipeliteLauncher pipeliteLauncher = createPipeliteLauncher(f.getPipelineName());
    new PipeliteServiceManager().addService(pipeliteLauncher).runSync();

    ProcessRunnerMetrics metrics = pipeliteLauncher.getMetrics();
    assertThat(metrics.getInternalErrorCount()).isEqualTo(0);
    assertThat(metrics.getProcessCompletedCount()).isZero();
    assertThat(metrics.getProcessFailedCount()).isEqualTo(PROCESS_CNT);
    assertThat(metrics.getStageFailedCount()).isEqualTo(PROCESS_CNT);
    assertThat(metrics.getStageSuccessCount()).isEqualTo(0);

    assertThat(f.stageExecutor.submitCount.get()).isEqualTo(PROCESS_CNT);
    assertThat(f.stageExecutor.pollCount.get()).isEqualTo(0);
  }

  public void testPollError() {
    TestProcessFactory<PollErrorExecutor> f = pollError;

    PipeliteLauncher pipeliteLauncher = createPipeliteLauncher(f.getPipelineName());
    new PipeliteServiceManager().addService(pipeliteLauncher).runSync();

    ProcessRunnerMetrics metrics = pipeliteLauncher.getMetrics();
    assertThat(metrics.getInternalErrorCount()).isEqualTo(0);
    assertThat(metrics.getProcessCompletedCount()).isZero();
    assertThat(metrics.getProcessFailedCount()).isEqualTo(PROCESS_CNT);
    assertThat(metrics.getStageFailedCount()).isEqualTo(PROCESS_CNT);
    assertThat(metrics.getStageSuccessCount()).isEqualTo(0);

    assertThat(f.stageExecutor.submitCount.get()).isEqualTo(PROCESS_CNT);
    assertThat(f.stageExecutor.pollCount.get()).isEqualTo(PROCESS_CNT);
  }

  public void testPollException() {
    TestProcessFactory<PollExceptionExecutor> f = pollException;

    PipeliteLauncher pipeliteLauncher = createPipeliteLauncher(f.getPipelineName());
    new PipeliteServiceManager().addService(pipeliteLauncher).runSync();

    ProcessRunnerMetrics metrics = pipeliteLauncher.getMetrics();
    assertThat(metrics.getInternalErrorCount()).isEqualTo(0);
    assertThat(metrics.getProcessCompletedCount()).isZero();
    assertThat(metrics.getProcessFailedCount()).isEqualTo(PROCESS_CNT);
    assertThat(metrics.getStageFailedCount()).isEqualTo(PROCESS_CNT);
    assertThat(metrics.getStageSuccessCount()).isEqualTo(0);

    assertThat(f.stageExecutor.submitCount.get()).isEqualTo(PROCESS_CNT);
    assertThat(f.stageExecutor.pollCount.get()).isEqualTo(PROCESS_CNT);
  }
}
