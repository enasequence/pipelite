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
import lombok.Value;
import pipelite.TestInMemoryProcessFactory;
import pipelite.UniqueStringGenerator;
import pipelite.configuration.LauncherConfiguration;
import pipelite.configuration.ProcessConfiguration;
import pipelite.configuration.StageConfiguration;
import pipelite.entity.ProcessEntity;
import pipelite.entity.StageEntity;
import pipelite.executor.PollableExecutor;
import pipelite.executor.StageExecutor;
import pipelite.process.Process;
import pipelite.process.ProcessExecutionState;
import pipelite.process.builder.ProcessBuilder;
import pipelite.service.LockService;
import pipelite.service.ProcessService;
import pipelite.service.StageService;
import pipelite.stage.Stage;
import pipelite.stage.StageExecutionResult;

public class PipeliteLauncherPollableTester {

  private final LauncherConfiguration launcherConfiguration;
  private final ProcessConfiguration processConfiguration;
  private final StageConfiguration stageConfiguration;
  private final ProcessService processService;
  private final StageService stageService;
  private final LockService lockService;

  public PipeliteLauncherPollableTester(
      LauncherConfiguration launcherConfiguration,
      ProcessConfiguration processConfiguration,
      StageConfiguration stageConfiguration,
      ProcessService processService,
      StageService stageService,
      LockService lockService) {
    this.launcherConfiguration = launcherConfiguration;
    this.processConfiguration = processConfiguration;
    this.stageConfiguration = stageConfiguration;
    this.processService = processService;
    this.stageService = stageService;
    this.lockService = lockService;

    pollCount = new AtomicInteger();
    executeCount = new AtomicInteger();
    pollCount = new AtomicInteger();
    executeCount = new AtomicInteger();
    pollCount = new AtomicInteger();
    executeCount = new AtomicInteger();
  }

  private static final int PROCESS_CNT = 4;

  private static AtomicInteger pollCount;
  private static AtomicInteger executeCount;

  private PipeliteLauncher pipeliteLauncher() {
    PipeliteLauncher pipeliteLauncher =
        new PipeliteLauncher(
            launcherConfiguration,
            processConfiguration,
            stageConfiguration,
            processService,
            stageService,
            lockService);
    pipeliteLauncher.setShutdownIfIdle(true);
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

  private void init(ProcessExecutionState processExecutionState, StageExecutor stageExecutor) {
    List<Process> processes = new ArrayList<>();

    String pipelineName = UniqueStringGenerator.randomPipelineName();
    for (int i = 0; i < PROCESS_CNT; ++i) {
      String processId = UniqueStringGenerator.randomProcessId();
      String stageName = UniqueStringGenerator.randomStageName();
      ProcessEntity processEntity = ProcessEntity.newExecution(processId, pipelineName, 1);
      processEntity.setState(processExecutionState);
      processService.saveProcess(processEntity);

      Process process =
          new ProcessBuilder(pipelineName, processId, 9)
              .execute(stageName)
              .with(stageExecutor)
              .build();
      processes.add(process);

      Stage stage = process.getStages().get(0);
      StageEntity stageEntity = StageEntity.createExecution(stage);
      stageEntity.startExecution(stage);
      stageService.saveStage(stageEntity);
    }

    TestInMemoryProcessFactory processFactory =
        new TestInMemoryProcessFactory(pipelineName, processes);
    processConfiguration.setProcessFactory(processFactory);
  }

  public void testPollSuccessExecuteSuccess() {
    init(ProcessExecutionState.ACTIVE, new PollSuccessExecuteSuccessStageExecutor());

    PipeliteLauncher pipeliteLauncher = pipeliteLauncher();
    ServerManager.run(pipeliteLauncher, pipeliteLauncher.serviceName());

    assertThat(pipeliteLauncher.getStageFailedCount()).isEqualTo(0);
    assertThat(pipeliteLauncher.getStageCompletedCount()).isEqualTo(PROCESS_CNT);
    assertThat(pollCount.get()).isEqualTo(PROCESS_CNT);
    assertThat(executeCount.get()).isEqualTo(0);
  }

  public void testPollErrorExecuteSuccess() {
    init(ProcessExecutionState.ACTIVE, new PollErrorExecuteSuccessStageExecutor());

    PipeliteLauncher pipeliteLauncher = pipeliteLauncher();
    ServerManager.run(pipeliteLauncher, pipeliteLauncher.serviceName());

    assertThat(pipeliteLauncher.getStageFailedCount()).isEqualTo(0);
    assertThat(pipeliteLauncher.getStageCompletedCount()).isEqualTo(PROCESS_CNT);
    assertThat(pollCount.get()).isEqualTo(PROCESS_CNT);
    assertThat(executeCount.get()).isEqualTo(PROCESS_CNT);
  }

  public void testPollErrorExecuteError() {
    init(ProcessExecutionState.ACTIVE, new PollErrorExecuteErrorStageExecutor());

    PipeliteLauncher pipeliteLauncher = pipeliteLauncher();
    ServerManager.run(pipeliteLauncher, pipeliteLauncher.serviceName());

    assertThat(pipeliteLauncher.getStageFailedCount()).isEqualTo(PROCESS_CNT);
    assertThat(pipeliteLauncher.getStageCompletedCount()).isEqualTo(0);
    assertThat(pollCount.get()).isEqualTo(PROCESS_CNT);
    assertThat(executeCount.get()).isEqualTo(PROCESS_CNT);
  }

  public void testPollExceptionExecuteSuccess() {
    init(ProcessExecutionState.ACTIVE, new PollExceptionExecuteSuccessStageExecutor());

    PipeliteLauncher pipeliteLauncher = pipeliteLauncher();
    ServerManager.run(pipeliteLauncher, pipeliteLauncher.serviceName());

    assertThat(pipeliteLauncher.getStageFailedCount()).isEqualTo(0);
    assertThat(pipeliteLauncher.getStageCompletedCount()).isEqualTo(PROCESS_CNT);
    assertThat(pollCount.get()).isEqualTo(PROCESS_CNT);
    assertThat(executeCount.get()).isEqualTo(PROCESS_CNT);
  }

  public void testPollExceptionExecuteError() {
    init(ProcessExecutionState.ACTIVE, new PollExceptionExecuteErrorStageExecutor());

    PipeliteLauncher pipeliteLauncher = pipeliteLauncher();
    ServerManager.run(pipeliteLauncher, pipeliteLauncher.serviceName());

    assertThat(pipeliteLauncher.getStageFailedCount()).isEqualTo(PROCESS_CNT);
    assertThat(pipeliteLauncher.getStageCompletedCount()).isEqualTo(0);
    assertThat(pollCount.get()).isEqualTo(PROCESS_CNT);
    assertThat(executeCount.get()).isEqualTo(PROCESS_CNT);
  }
}
