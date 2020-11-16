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

import lombok.Value;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;
import pipelite.TestInMemoryProcessFactory;
import pipelite.UniqueStringGenerator;
import pipelite.configuration.ProcessConfiguration;
import pipelite.entity.ProcessEntity;
import pipelite.entity.StageEntity;
import pipelite.executor.PollableExecutor;
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
public class PipeliteLauncherPollableTester {

  @Autowired private ProcessConfiguration processConfiguration;
  @Autowired private ProcessService processService;
  @Autowired private StageService stageService;
  @Autowired private ObjectProvider<PipeliteLauncher> pipeliteLauncherObjectProvider;

  @TestConfiguration
  static class TestConfig {
    @Bean
    public ProcessFactory pollSuccessExecuteSuccess() {
      return new TestInMemoryProcessFactory(
          POLL_SUCCESS_EXECUTE_SUCCESS_NAME, POLL_SUCCESS_EXECUTE_SUCCESS_PROCESSES);
    }

    @Bean
    public ProcessFactory pollErrorExecuteSuccess() {
      return new TestInMemoryProcessFactory(
          POLL_ERROR_EXECUTE_SUCCESS_NAME, POLL_ERROR_EXECUTE_SUCCESS_PROCESSES);
    }

    @Bean
    public ProcessFactory pollErrorExecuteError() {
      return new TestInMemoryProcessFactory(
          POLL_ERROR_EXECUTE_ERROR_NAME, POLL_ERROR_EXECUTE_ERROR_PROCESSES);
    }

    @Bean
    public ProcessFactory pollExceptionExecuteSuccess() {
      return new TestInMemoryProcessFactory(
          POLL_EXCEPTION_EXECUTE_SUCCESS_NAME, POLL_EXCEPTION_EXECUTE_SUCCESS_PROCESSES);
    }

    @Bean
    public ProcessFactory pollExceptionExecuteError() {
      return new TestInMemoryProcessFactory(
          POLL_EXCEPTION_EXECUTE_ERROR_NAME, POLL_EXCEPTION_EXECUTE_ERROR_PROCESSES);
    }
  }

  private static AtomicInteger pollCount = new AtomicInteger();
  private static AtomicInteger executeCount = new AtomicInteger();

  private static final String POLL_SUCCESS_EXECUTE_SUCCESS_NAME =
      UniqueStringGenerator.randomPipelineName();
  private static final String POLL_ERROR_EXECUTE_SUCCESS_NAME =
      UniqueStringGenerator.randomPipelineName();
  private static final String POLL_ERROR_EXECUTE_ERROR_NAME =
      UniqueStringGenerator.randomPipelineName();
  private static final String POLL_EXCEPTION_EXECUTE_SUCCESS_NAME =
      UniqueStringGenerator.randomPipelineName();
  private static final String POLL_EXCEPTION_EXECUTE_ERROR_NAME =
      UniqueStringGenerator.randomPipelineName();

  private static final int PROCESS_CNT = 4;
  private static final List<Process> POLL_SUCCESS_EXECUTE_SUCCESS_PROCESSES = new ArrayList<>();
  private static final List<Process> POLL_ERROR_EXECUTE_SUCCESS_PROCESSES = new ArrayList<>();
  private static final List<Process> POLL_ERROR_EXECUTE_ERROR_PROCESSES = new ArrayList<>();
  private static final List<Process> POLL_EXCEPTION_EXECUTE_SUCCESS_PROCESSES = new ArrayList<>();
  private static final List<Process> POLL_EXCEPTION_EXECUTE_ERROR_PROCESSES = new ArrayList<>();

  static {
    IntStream.range(0, PROCESS_CNT)
        .forEach(
            i -> {
              String processId = UniqueStringGenerator.randomProcessId();
              String stageName = UniqueStringGenerator.randomStageName();
              POLL_SUCCESS_EXECUTE_SUCCESS_PROCESSES.add(
                  new ProcessBuilder(POLL_SUCCESS_EXECUTE_SUCCESS_NAME, processId, 9)
                      .execute(stageName)
                      .with(new PollSuccessExecuteSuccessStageExecutor())
                      .build());
              POLL_ERROR_EXECUTE_SUCCESS_PROCESSES.add(
                  new ProcessBuilder(POLL_ERROR_EXECUTE_SUCCESS_NAME, processId, 9)
                      .execute(stageName)
                      .with(new PollErrorExecuteSuccessStageExecutor())
                      .build());
              POLL_ERROR_EXECUTE_ERROR_PROCESSES.add(
                  new ProcessBuilder(POLL_ERROR_EXECUTE_ERROR_NAME, processId, 9)
                      .execute(stageName)
                      .with(new PollErrorExecuteErrorStageExecutor())
                      .build());
              POLL_EXCEPTION_EXECUTE_SUCCESS_PROCESSES.add(
                  new ProcessBuilder(POLL_EXCEPTION_EXECUTE_SUCCESS_NAME, processId, 9)
                      .execute(stageName)
                      .with(new PollExceptionExecuteSuccessStageExecutor())
                      .build());
              POLL_EXCEPTION_EXECUTE_ERROR_PROCESSES.add(
                  new ProcessBuilder(POLL_EXCEPTION_EXECUTE_ERROR_NAME, processId, 9)
                      .execute(stageName)
                      .with(new PollExceptionExecuteErrorStageExecutor())
                      .build());
            });
  }

  private PipeliteLauncher pipeliteLauncher() {
    PipeliteLauncher pipeliteLauncher = pipeliteLauncherObjectProvider.getObject();
    return pipeliteLauncher;
  }

  @Value
  public static class PollSuccessExecuteSuccessStageExecutor implements PollableExecutor {
    @Override
    public StageExecutionResult execute(Stage stage) {
      executeCount.incrementAndGet();
      return StageExecutionResult.success();
    }

    @Override
    public StageExecutionResult poll(Stage stage) {
      pollCount.incrementAndGet();
      return StageExecutionResult.success();
    }
  }

  @Value
  public static class PollErrorExecuteSuccessStageExecutor implements PollableExecutor {
    @Override
    public StageExecutionResult execute(Stage stage) {
      executeCount.incrementAndGet();
      return StageExecutionResult.success();
    }

    @Override
    public StageExecutionResult poll(Stage stage) {
      pollCount.incrementAndGet();
      return StageExecutionResult.error();
    }
  }

  @Value
  public static class PollErrorExecuteErrorStageExecutor implements PollableExecutor {
    @Override
    public StageExecutionResult execute(Stage stage) {
      executeCount.incrementAndGet();
      return StageExecutionResult.error();
    }

    @Override
    public StageExecutionResult poll(Stage stage) {
      pollCount.incrementAndGet();
      return StageExecutionResult.error();
    }
  }

  @Value
  public static class PollExceptionExecuteSuccessStageExecutor implements PollableExecutor {
    @Override
    public StageExecutionResult execute(Stage stage) {
      executeCount.incrementAndGet();
      return StageExecutionResult.success();
    }

    @Override
    public StageExecutionResult poll(Stage stage) {
      pollCount.incrementAndGet();
      throw new RuntimeException();
    }
  }

  @Value
  public static class PollExceptionExecuteErrorStageExecutor implements PollableExecutor {
    @Override
    public StageExecutionResult execute(Stage stage) {
      executeCount.incrementAndGet();
      return StageExecutionResult.error();
    }

    @Override
    public StageExecutionResult poll(Stage stage) {
      pollCount.incrementAndGet();
      throw new RuntimeException();
    }
  }

  private void init(ProcessState processState, List<Process> processes) {
    pollCount.set(0);
    executeCount.set(0);

    for (Process process : processes) {
      ProcessEntity processEntity =
          ProcessEntity.newExecution(process.getProcessId(), process.getPipelineName(), 1);
      processEntity.setState(processState);
      processService.saveProcess(processEntity);

      Stage stage = process.getStages().get(0);
      StageEntity stageEntity = StageEntity.createExecution(stage);
      stageEntity.startExecution(stage);
      stageService.saveStage(stageEntity);

      processConfiguration.setPipelineName(process.getPipelineName());
    }
  }

  public void testPollSuccessExecuteSuccess() {
    init(ProcessState.ACTIVE, POLL_SUCCESS_EXECUTE_SUCCESS_PROCESSES);

    PipeliteLauncher pipeliteLauncher = pipeliteLauncher();
    ServerManager.run(pipeliteLauncher, pipeliteLauncher.serviceName());

    assertThat(pipeliteLauncher.getStageFailedCount()).isEqualTo(0);
    assertThat(pipeliteLauncher.getStageCompletedCount()).isEqualTo(PROCESS_CNT);
    assertThat(pollCount.get()).isEqualTo(PROCESS_CNT);
    assertThat(executeCount.get()).isEqualTo(0);
  }

  public void testPollErrorExecuteSuccess() {
    init(ProcessState.ACTIVE, POLL_ERROR_EXECUTE_SUCCESS_PROCESSES);

    PipeliteLauncher pipeliteLauncher = pipeliteLauncher();
    ServerManager.run(pipeliteLauncher, pipeliteLauncher.serviceName());

    assertThat(pipeliteLauncher.getStageFailedCount()).isEqualTo(0);
    assertThat(pipeliteLauncher.getStageCompletedCount()).isEqualTo(PROCESS_CNT);
    assertThat(pollCount.get()).isEqualTo(PROCESS_CNT);
    assertThat(executeCount.get()).isEqualTo(PROCESS_CNT);
  }

  public void testPollErrorExecuteError() {
    init(ProcessState.ACTIVE, POLL_ERROR_EXECUTE_ERROR_PROCESSES);

    PipeliteLauncher pipeliteLauncher = pipeliteLauncher();
    ServerManager.run(pipeliteLauncher, pipeliteLauncher.serviceName());

    assertThat(pipeliteLauncher.getStageFailedCount()).isEqualTo(PROCESS_CNT);
    assertThat(pipeliteLauncher.getStageCompletedCount()).isEqualTo(0);
    assertThat(pollCount.get()).isEqualTo(PROCESS_CNT);
    assertThat(executeCount.get()).isEqualTo(PROCESS_CNT);
  }

  public void testPollExceptionExecuteSuccess() {
    init(ProcessState.ACTIVE, POLL_EXCEPTION_EXECUTE_SUCCESS_PROCESSES);

    PipeliteLauncher pipeliteLauncher = pipeliteLauncher();
    ServerManager.run(pipeliteLauncher, pipeliteLauncher.serviceName());

    assertThat(pipeliteLauncher.getStageFailedCount()).isEqualTo(0);
    assertThat(pipeliteLauncher.getStageCompletedCount()).isEqualTo(PROCESS_CNT);
    assertThat(pollCount.get()).isEqualTo(PROCESS_CNT);
    assertThat(executeCount.get()).isEqualTo(PROCESS_CNT);
  }

  public void testPollExceptionExecuteError() {
    init(ProcessState.ACTIVE, POLL_EXCEPTION_EXECUTE_ERROR_PROCESSES);

    PipeliteLauncher pipeliteLauncher = pipeliteLauncher();
    ServerManager.run(pipeliteLauncher, pipeliteLauncher.serviceName());

    assertThat(pipeliteLauncher.getStageFailedCount()).isEqualTo(PROCESS_CNT);
    assertThat(pipeliteLauncher.getStageCompletedCount()).isEqualTo(0);
    assertThat(pollCount.get()).isEqualTo(PROCESS_CNT);
    assertThat(executeCount.get()).isEqualTo(PROCESS_CNT);
  }
}
