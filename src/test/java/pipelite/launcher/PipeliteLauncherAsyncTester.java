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
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;
import pipelite.TestProcessFactory;
import pipelite.UniqueStringGenerator;
import pipelite.configuration.LauncherConfiguration;
import pipelite.entity.ProcessEntity;
import pipelite.entity.StageEntity;
import pipelite.executor.StageExecutor;
import pipelite.process.Process;
import pipelite.process.ProcessFactory;
import pipelite.process.ProcessState;
import pipelite.process.builder.ProcessBuilder;
import pipelite.service.ProcessService;
import pipelite.service.StageService;
import pipelite.stage.Stage;
import pipelite.stage.StageExecutionResult;

@Component
@Scope("prototype")
public class PipeliteLauncherAsyncTester {

  @Autowired private LauncherConfiguration launcherConfiguration;
  @Autowired private ProcessService processService;
  @Autowired private StageService stageService;
  @Autowired private ObjectProvider<PipeliteLauncher> pipeliteLauncherObjectProvider;

  @TestConfiguration
  static class TestConfig {
    @Bean
    public ProcessFactory submitSuccessPollSuccess() {
      return new TestProcessFactory(
          SUBMIT_SUCCESS_POLL_SUCCESS_PROCESS_NAME, SUBMIT_SUCCESS_POLL_SUCCESS_PROCESSES);
    }

    @Bean
    public ProcessFactory submitError() {
      return new TestProcessFactory(SUBMIT_ERROR_PROCESS_NAME, SUBMIT_ERROR_PROCESSES);
    }

    @Bean
    public ProcessFactory submitException() {
      return new TestProcessFactory(
          SUBMIT_EXCEPTION_PROCESS_NAME, SUBMIT_EXCEPTION_PROCESSES);
    }

    @Bean
    public ProcessFactory pollError() {
      return new TestProcessFactory(POLL_ERROR_PROCESS_NAME, POLL_ERROR_PROCESSES);
    }

    @Bean
    public ProcessFactory pollException() {
      return new TestProcessFactory(POLL_EXCEPTION_PROCESS_NAME, POLL_EXCEPTION_PROCESSES);
    }
  }

  private static AtomicInteger submitCount = new AtomicInteger();
  private static AtomicInteger pollCount = new AtomicInteger();

  private static final String SUBMIT_SUCCESS_POLL_SUCCESS_PROCESS_NAME =
      UniqueStringGenerator.randomPipelineName();
  private static final String SUBMIT_ERROR_PROCESS_NAME =
      UniqueStringGenerator.randomPipelineName();
  private static final String POLL_EXCEPTION_PROCESS_NAME =
      UniqueStringGenerator.randomPipelineName();
  private static final String SUBMIT_EXCEPTION_PROCESS_NAME =
      UniqueStringGenerator.randomPipelineName();
  private static final String POLL_ERROR_PROCESS_NAME = UniqueStringGenerator.randomPipelineName();

  private static final int PROCESS_CNT = 4;
  private static final List<Process> SUBMIT_SUCCESS_POLL_SUCCESS_PROCESSES = new ArrayList<>();
  private static final List<Process> SUBMIT_ERROR_PROCESSES = new ArrayList<>();
  private static final List<Process> POLL_EXCEPTION_PROCESSES = new ArrayList<>();
  private static final List<Process> SUBMIT_EXCEPTION_PROCESSES = new ArrayList<>();
  private static final List<Process> POLL_ERROR_PROCESSES = new ArrayList<>();

  static {
    IntStream.range(0, PROCESS_CNT)
        .forEach(
            i -> {
              String processId = UniqueStringGenerator.randomProcessId();
              String stageName = UniqueStringGenerator.randomStageName();
              SUBMIT_SUCCESS_POLL_SUCCESS_PROCESSES.add(
                  new ProcessBuilder(processId)
                      .execute(stageName)
                      .with(new SubmitSuccessPollSuccessStageExecutor())
                      .build());
              SUBMIT_ERROR_PROCESSES.add(
                  new ProcessBuilder(processId)
                      .execute(stageName)
                      .with(new SubmitErrorStageExecutor())
                      .build());
              SUBMIT_EXCEPTION_PROCESSES.add(
                  new ProcessBuilder(processId)
                      .execute(stageName)
                      .with(new SubmitExceptionStageExecutor())
                      .build());
              POLL_ERROR_PROCESSES.add(
                  new ProcessBuilder(processId)
                      .execute(stageName)
                      .with(new PollErrorStageExecutor())
                      .build());
              POLL_EXCEPTION_PROCESSES.add(
                  new ProcessBuilder(processId)
                      .execute(stageName)
                      .with(new PollExceptionStageExecutor())
                      .build());
            });
  }

  private PipeliteLauncher pipeliteLauncher() {
    PipeliteLauncher pipeliteLauncher = pipeliteLauncherObjectProvider.getObject();
    return pipeliteLauncher;
  }

  public static class SubmitSuccessPollSuccessStageExecutor implements StageExecutor {
    private boolean submit;

    @Override
    public StageExecutionResult execute(String pipelineName, String processId, Stage stage) {
      if (!submit) {
        submitCount.incrementAndGet();
        submit = true;
        return StageExecutionResult.active();
      } else {
        pollCount.incrementAndGet();
        if (pollCount.get() > 1) {
          boolean stop = true;
        }
        return StageExecutionResult.success();
      }
    }
  }

  public static class SubmitErrorStageExecutor implements StageExecutor {
    private boolean submit;

    @Override
    public StageExecutionResult execute(String pipelineName, String processId, Stage stage) {
      if (!submit) {
        submitCount.incrementAndGet();
        submit = true;
        return StageExecutionResult.error();
      } else {
        throw new RuntimeException("Unexpected call to execute");
      }
    }
  }

  public static class SubmitExceptionStageExecutor implements StageExecutor {
    private boolean submit;

    @Override
    public StageExecutionResult execute(String pipelineName, String processId, Stage stage) {
      if (!submit) {
        submitCount.incrementAndGet();
        submit = true;
        throw new RuntimeException("Expected exception from submit");
      } else {
        throw new RuntimeException("Unexpected call to execute");
      }
    }
  }

  public static class PollErrorStageExecutor implements StageExecutor {
    private boolean submit;

    @Override
    public StageExecutionResult execute(String pipelineName, String processId, Stage stage) {
      if (!submit) {
        submitCount.incrementAndGet();
        submit = true;
        return StageExecutionResult.active();
      } else {
        pollCount.incrementAndGet();
        return StageExecutionResult.error();
      }
    }
  }

  public static class PollExceptionStageExecutor implements StageExecutor {
    private boolean submit;

    @Override
    public StageExecutionResult execute(String pipelineName, String processId, Stage stage) {
      if (!submit) {
        submitCount.incrementAndGet();
        submit = true;
        return StageExecutionResult.active();
      } else {
        pollCount.incrementAndGet();
        throw new RuntimeException("Expected exception from poll");
      }
    }
  }

  private void init(String pipelineName, ProcessState processState, List<Process> processes) {
    submitCount.set(0);
    pollCount.set(0);

    for (Process process : processes) {
      ProcessEntity processEntity =
          ProcessEntity.newExecution(pipelineName, process.getProcessId(), 1);
      processEntity.setState(processState);
      processService.saveProcess(processEntity);

      Stage stage = process.getStages().get(0);
      StageEntity stageEntity = StageEntity.createExecution(pipelineName, process.getProcessId(), stage);
      stageEntity.startExecution(stage);
      stageService.saveStage(stageEntity);

      launcherConfiguration.setPipelineName(pipelineName);
    }
  }

  public void testSubmitSuccessPollSuccess() {
    init(
        SUBMIT_SUCCESS_POLL_SUCCESS_PROCESS_NAME,
        ProcessState.ACTIVE,
        SUBMIT_SUCCESS_POLL_SUCCESS_PROCESSES);

    PipeliteLauncher pipeliteLauncher = pipeliteLauncher();
    ServerManager.run(pipeliteLauncher, pipeliteLauncher.serviceName());

    assertThat(pipeliteLauncher.getStats().getStageFailedCount()).isEqualTo(0);
    assertThat(pipeliteLauncher.getStats().getStageSuccessCount()).isEqualTo(PROCESS_CNT);
    assertThat(submitCount.get()).isEqualTo(PROCESS_CNT);
    assertThat(pollCount.get()).isEqualTo(PROCESS_CNT);
  }

  public void testSubmitError() {
    init(SUBMIT_ERROR_PROCESS_NAME, ProcessState.ACTIVE, SUBMIT_ERROR_PROCESSES);

    PipeliteLauncher pipeliteLauncher = pipeliteLauncher();
    ServerManager.run(pipeliteLauncher, pipeliteLauncher.serviceName());

    assertThat(pipeliteLauncher.getStats().getStageFailedCount()).isEqualTo(PROCESS_CNT);
    assertThat(pipeliteLauncher.getStats().getStageSuccessCount()).isEqualTo(0);
    assertThat(submitCount.get()).isEqualTo(PROCESS_CNT);
    assertThat(pollCount.get()).isEqualTo(0);
  }

  public void testSubmitException() {
    init(SUBMIT_EXCEPTION_PROCESS_NAME, ProcessState.ACTIVE, SUBMIT_EXCEPTION_PROCESSES);

    PipeliteLauncher pipeliteLauncher = pipeliteLauncher();
    ServerManager.run(pipeliteLauncher, pipeliteLauncher.serviceName());

    assertThat(pipeliteLauncher.getStats().getStageFailedCount()).isEqualTo(PROCESS_CNT);
    assertThat(pipeliteLauncher.getStats().getStageSuccessCount()).isEqualTo(0);
    assertThat(submitCount.get()).isEqualTo(PROCESS_CNT);
    assertThat(pollCount.get()).isEqualTo(0);
  }

  public void testPollError() {
    init(POLL_ERROR_PROCESS_NAME, ProcessState.ACTIVE, POLL_ERROR_PROCESSES);

    PipeliteLauncher pipeliteLauncher = pipeliteLauncher();
    ServerManager.run(pipeliteLauncher, pipeliteLauncher.serviceName());

    assertThat(pipeliteLauncher.getStats().getStageFailedCount()).isEqualTo(PROCESS_CNT);
    assertThat(pipeliteLauncher.getStats().getStageSuccessCount()).isEqualTo(0);
    assertThat(submitCount.get()).isEqualTo(PROCESS_CNT);
    assertThat(pollCount.get()).isEqualTo(PROCESS_CNT);
  }

  public void testPollException() {
    init(POLL_EXCEPTION_PROCESS_NAME, ProcessState.ACTIVE, POLL_EXCEPTION_PROCESSES);

    PipeliteLauncher pipeliteLauncher = pipeliteLauncher();
    ServerManager.run(pipeliteLauncher, pipeliteLauncher.serviceName());

    assertThat(pipeliteLauncher.getStats().getStageFailedCount()).isEqualTo(PROCESS_CNT);
    assertThat(pipeliteLauncher.getStats().getStageSuccessCount()).isEqualTo(0);
    assertThat(submitCount.get()).isEqualTo(PROCESS_CNT);
    assertThat(pollCount.get()).isEqualTo(PROCESS_CNT);
  }
}
