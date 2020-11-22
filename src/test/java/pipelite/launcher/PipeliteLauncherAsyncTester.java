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

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import lombok.Value;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;
import pipelite.TestProcessSource;
import pipelite.UniqueStringGenerator;
import pipelite.configuration.LauncherConfiguration;
import pipelite.executor.StageExecutor;
import pipelite.process.Process;
import pipelite.process.ProcessFactory;
import pipelite.process.ProcessSource;
import pipelite.process.builder.ProcessBuilder;
import pipelite.service.ProcessService;
import pipelite.service.StageService;
import pipelite.stage.Stage;
import pipelite.stage.StageExecutionResult;
import pipelite.stage.StageParameters;

@Component
@Scope("prototype")
public class PipeliteLauncherAsyncTester {

  private static final int PROCESS_CNT = 5;

  @Autowired private LauncherConfiguration launcherConfiguration;
  @Autowired private ProcessService processService;
  @Autowired private StageService stageService;
  @Autowired private ObjectProvider<PipeliteLauncher> pipeliteLauncherObjectProvider;

  @Autowired
  // @Qualifier("submitSuccessPollSuccess")
  private TestProcessFactory<SubmitSuccessPollSuccessExecutor> submitSuccessPollSuccess;

  @Autowired
  // @Qualifier("submitError")
  public TestProcessFactory<SubmitErrorExecutor> submitError;

  @Autowired
  // @Qualifier("submitException")
  public TestProcessFactory<SubmitExceptionExecutor> submitException;

  @Autowired
  // @Qualifier("pollError")
  public TestProcessFactory<PollErrorExecutor> pollError;

  @Autowired
  // @Qualifier("pollException")
  public TestProcessFactory<PollExceptionExecutor> pollException;

  @TestConfiguration
  static class TestConfig {
    @Bean // ("submitSuccessPollSuccess")
    // @Primary
    public TestProcessFactory<SubmitSuccessPollSuccessExecutor> submitSuccessPollSuccess() {
      return new TestProcessFactory(new SubmitSuccessPollSuccessExecutor());
    }

    @Bean // ("submitError")
    public TestProcessFactory<SubmitErrorExecutor> submitError() {
      return new TestProcessFactory(new SubmitErrorExecutor());
    }

    @Bean // ("submitException")
    public TestProcessFactory<SubmitExceptionExecutor> submitException() {
      return new TestProcessFactory(new SubmitExceptionExecutor());
    }

    @Bean // ("pollError")
    public TestProcessFactory<PollErrorExecutor> pollError() {
      return new TestProcessFactory(new PollErrorExecutor());
    }

    @Bean // ("pollException")
    public TestProcessFactory<PollExceptionExecutor> pollException() {
      return new TestProcessFactory(new PollExceptionExecutor());
    }

    @Bean
    public ProcessSource submitSuccessPollSuccessSource(
        @Autowired // @Qualifier("submitSuccessPollSuccess")
            TestProcessFactory<SubmitSuccessPollSuccessExecutor> f) {
      return new TestProcessSource(f.getPipelineName(), PROCESS_CNT);
    }

    @Bean
    public ProcessSource submitErrorSource(
        @Autowired
            // @Qualifier("submitError")
            TestProcessFactory<SubmitErrorExecutor> f) {
      return new TestProcessSource(f.getPipelineName(), PROCESS_CNT);
    }

    @Bean
    public ProcessSource submitExceptionSource(
        @Autowired
            // @Qualifier("submitException")
            TestProcessFactory<SubmitExceptionExecutor> f) {
      return new TestProcessSource(f.getPipelineName(), PROCESS_CNT);
    }

    @Bean
    public ProcessSource pollErrorSource(
        @Autowired
            // @Qualifier("pollError")
            TestProcessFactory<PollErrorExecutor> f) {
      return new TestProcessSource(f.getPipelineName(), PROCESS_CNT);
    }

    @Bean
    public ProcessSource pollExceptionSource(
        @Autowired
            // @Qualifier("pollException")
            TestProcessFactory<PollExceptionExecutor> f) {
      return new TestProcessSource(f.getPipelineName(), PROCESS_CNT);
    }
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
    public Process create(String processId) {
      processIds.add(processId);
      StageParameters stageParams =
          StageParameters.builder().immediateRetries(0).maximumRetries(0).build();
      return new ProcessBuilder(processId)
          .execute("STAGE", stageParams)
          .with(stageExecutor)
          .build();
    }
  }

  public abstract static class TestExecutor implements StageExecutor {
    public AtomicInteger submitCount = new AtomicInteger();
    public AtomicInteger pollCount = new AtomicInteger();
    private Set<String> submit = new ConcurrentHashMap<>().newKeySet();

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
    public StageExecutionResult execute(String pipelineName, String processId, Stage stage) {
      if (!isSubmitted(processId)) {
        submitCount.incrementAndGet();
        return StageExecutionResult.active();
      } else {
        pollCount.incrementAndGet();
        return StageExecutionResult.success();
      }
    }
  }

  public static class SubmitErrorExecutor extends TestExecutor {
    @Override
    public StageExecutionResult execute(String pipelineName, String processId, Stage stage) {
      if (!isSubmitted(processId)) {
        submitCount.incrementAndGet();
        return StageExecutionResult.error();
      } else {
        pollCount.incrementAndGet();
        throw new RuntimeException("Unexpected call to execute");
      }
    }
  }

  public static class SubmitExceptionExecutor extends TestExecutor {
    @Override
    public StageExecutionResult execute(String pipelineName, String processId, Stage stage) {
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
    public StageExecutionResult execute(String pipelineName, String processId, Stage stage) {
      if (!isSubmitted(processId)) {
        submitCount.incrementAndGet();
        return StageExecutionResult.active();
      } else {
        pollCount.incrementAndGet();
        return StageExecutionResult.error();
      }
    }
  }

  public static class PollExceptionExecutor extends TestExecutor {
    @Override
    public StageExecutionResult execute(String pipelineName, String processId, Stage stage) {
      if (!isSubmitted(processId)) {
        submitCount.incrementAndGet();
        return StageExecutionResult.active();
      } else {
        pollCount.incrementAndGet();
        throw new RuntimeException("Expected exception from poll");
      }
    }
  }

  public void testSubmitSuccessPollSuccess() {
    TestProcessFactory<SubmitSuccessPollSuccessExecutor> f = submitSuccessPollSuccess;

    launcherConfiguration.setPipelineName(f.getPipelineName());

    PipeliteLauncher pipeliteLauncher = pipeliteLauncherObjectProvider.getObject();
    ServerManager.run(pipeliteLauncher, pipeliteLauncher.serviceName());

    assertThat(pipeliteLauncher.getStats().getStageFailedCount()).isEqualTo(0);
    assertThat(pipeliteLauncher.getStats().getStageSuccessCount()).isEqualTo(PROCESS_CNT);
    assertThat(f.stageExecutor.submitCount.get()).isEqualTo(PROCESS_CNT);
    assertThat(f.stageExecutor.pollCount.get()).isEqualTo(PROCESS_CNT);
  }

  public void testSubmitError() {
    TestProcessFactory<SubmitErrorExecutor> f = submitError;

    launcherConfiguration.setPipelineName(submitError.getPipelineName());

    PipeliteLauncher pipeliteLauncher = pipeliteLauncherObjectProvider.getObject();
    ServerManager.run(pipeliteLauncher, pipeliteLauncher.serviceName());

    assertThat(pipeliteLauncher.getStats().getStageFailedCount()).isEqualTo(PROCESS_CNT);
    assertThat(pipeliteLauncher.getStats().getStageSuccessCount()).isEqualTo(0);
    assertThat(f.stageExecutor.submitCount.get()).isEqualTo(PROCESS_CNT);
    assertThat(f.stageExecutor.pollCount.get()).isEqualTo(0);
  }

  public void testSubmitException() {
    TestProcessFactory<SubmitExceptionExecutor> f = submitException;

    launcherConfiguration.setPipelineName(submitException.getPipelineName());

    PipeliteLauncher pipeliteLauncher = pipeliteLauncherObjectProvider.getObject();
    ServerManager.run(pipeliteLauncher, pipeliteLauncher.serviceName());

    assertThat(pipeliteLauncher.getStats().getStageFailedCount()).isEqualTo(PROCESS_CNT);
    assertThat(pipeliteLauncher.getStats().getStageSuccessCount()).isEqualTo(0);
    assertThat(f.stageExecutor.submitCount.get()).isEqualTo(PROCESS_CNT);
    assertThat(f.stageExecutor.pollCount.get()).isEqualTo(0);
  }

  public void testPollError() {
    TestProcessFactory<PollErrorExecutor> f = pollError;

    launcherConfiguration.setPipelineName(pollError.getPipelineName());

    PipeliteLauncher pipeliteLauncher = pipeliteLauncherObjectProvider.getObject();
    ServerManager.run(pipeliteLauncher, pipeliteLauncher.serviceName());

    assertThat(pipeliteLauncher.getStats().getStageFailedCount()).isEqualTo(PROCESS_CNT);
    assertThat(pipeliteLauncher.getStats().getStageSuccessCount()).isEqualTo(0);
    assertThat(f.stageExecutor.submitCount.get()).isEqualTo(PROCESS_CNT);
    assertThat(f.stageExecutor.pollCount.get()).isEqualTo(PROCESS_CNT);
  }

  public void testPollException() {
    TestProcessFactory<PollExceptionExecutor> f = pollException;

    launcherConfiguration.setPipelineName(pollException.getPipelineName());

    PipeliteLauncher pipeliteLauncher = pipeliteLauncherObjectProvider.getObject();
    ServerManager.run(pipeliteLauncher, pipeliteLauncher.serviceName());

    assertThat(pipeliteLauncher.getStats().getStageFailedCount()).isEqualTo(PROCESS_CNT);
    assertThat(pipeliteLauncher.getStats().getStageSuccessCount()).isEqualTo(0);
    assertThat(f.stageExecutor.submitCount.get()).isEqualTo(PROCESS_CNT);
    assertThat(f.stageExecutor.pollCount.get()).isEqualTo(PROCESS_CNT);
  }
}
