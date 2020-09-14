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
package pipelite.launcher.pipelite;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.Value;
import pipelite.TestInMemoryProcessFactory;
import pipelite.UniqueStringGenerator;
import pipelite.configuration.LauncherConfiguration;
import pipelite.configuration.ProcessConfiguration;
import pipelite.configuration.TaskConfiguration;
import pipelite.entity.ProcessEntity;
import pipelite.entity.TaskEntity;
import pipelite.executor.PollableExecutor;
import pipelite.executor.TaskExecutor;
import pipelite.launcher.ServerManager;
import pipelite.process.Process;
import pipelite.process.ProcessExecutionState;
import pipelite.process.builder.ProcessBuilder;
import pipelite.service.LockService;
import pipelite.service.ProcessService;
import pipelite.service.TaskService;
import pipelite.task.Task;
import pipelite.task.TaskExecutionResult;

public class PollablePipeliteLauncherTester {

  private final LauncherConfiguration launcherConfiguration;
  private final ProcessConfiguration processConfiguration;
  private final TaskConfiguration taskConfiguration;
  private final ProcessService processService;
  private final TaskService taskService;
  private final LockService lockService;

  public PollablePipeliteLauncherTester(
      LauncherConfiguration launcherConfiguration,
      ProcessConfiguration processConfiguration,
      TaskConfiguration taskConfiguration,
      ProcessService processService,
      TaskService taskService,
      LockService lockService) {
    this.launcherConfiguration = launcherConfiguration;
    this.processConfiguration = processConfiguration;
    this.taskConfiguration = taskConfiguration;
    this.processService = processService;
    this.taskService = taskService;
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
            taskConfiguration,
            processService,
            taskService,
            lockService);
    pipeliteLauncher.setShutdownIfIdle(true);
    return pipeliteLauncher;
  }

  @Value
  public static class PollSuccessExecuteSuccessTaskExecutor implements PollableExecutor {
    @Override
    public TaskExecutionResult execute(Task task) {
      executeCount.incrementAndGet();
      return TaskExecutionResult.success();
    }

    @Override
    public TaskExecutionResult poll(Task task) {
      pollCount.incrementAndGet();
      return TaskExecutionResult.success();
    }
  }

  @Value
  public static class PollErrorExecuteSuccessTaskExecutor implements PollableExecutor {
    @Override
    public TaskExecutionResult execute(Task task) {
      executeCount.incrementAndGet();
      return TaskExecutionResult.success();
    }

    @Override
    public TaskExecutionResult poll(Task task) {
      pollCount.incrementAndGet();
      return TaskExecutionResult.error();
    }
  }

  @Value
  public static class PollErrorExecuteErrorTaskExecutor implements PollableExecutor {
    @Override
    public TaskExecutionResult execute(Task task) {
      executeCount.incrementAndGet();
      return TaskExecutionResult.error();
    }

    @Override
    public TaskExecutionResult poll(Task task) {
      pollCount.incrementAndGet();
      return TaskExecutionResult.error();
    }
  }

  @Value
  public static class PollExceptionExecuteSuccessTaskExecutor implements PollableExecutor {
    @Override
    public TaskExecutionResult execute(Task task) {
      executeCount.incrementAndGet();
      return TaskExecutionResult.success();
    }

    @Override
    public TaskExecutionResult poll(Task task) {
      pollCount.incrementAndGet();
      throw new RuntimeException();
    }
  }

  @Value
  public static class PollExceptionExecuteErrorTaskExecutor implements PollableExecutor {
    @Override
    public TaskExecutionResult execute(Task task) {
      executeCount.incrementAndGet();
      return TaskExecutionResult.error();
    }

    @Override
    public TaskExecutionResult poll(Task task) {
      pollCount.incrementAndGet();
      throw new RuntimeException();
    }
  }

  private void init(ProcessExecutionState processExecutionState, TaskExecutor taskExecutor) {
    List<Process> processes = new ArrayList<>();

    for (int i = 0; i < PROCESS_CNT; ++i) {
      String processName = processConfiguration.getProcessName();
      String processId = UniqueStringGenerator.randomProcessId();
      String taskName = UniqueStringGenerator.randomTaskName();
      ProcessEntity processEntity = ProcessEntity.newExecution(processId, processName, 1);
      processEntity.setState(processExecutionState);
      processService.saveProcess(processEntity);

      Process process =
          new ProcessBuilder(processName, processId, 9)
              .task(taskName)
              .executor(taskExecutor)
              .build();
      processes.add(process);

      Task task = process.getTasks().get(0);
      TaskEntity taskEntity = TaskEntity.createExecution(task);
      taskEntity.startExecution(task);
      taskService.saveTask(taskEntity);
    }

    TestInMemoryProcessFactory processFactory = new TestInMemoryProcessFactory(processes);
    processConfiguration.setProcessFactory(processFactory);
  }

  public void testPollSuccessExecuteSuccess() {
    init(ProcessExecutionState.ACTIVE, new PollSuccessExecuteSuccessTaskExecutor());

    PipeliteLauncher pipeliteLauncher = pipeliteLauncher();
    ServerManager.run(pipeliteLauncher, pipeliteLauncher.serviceName());

    assertThat(pipeliteLauncher.getTaskFailedCount()).isEqualTo(0);
    assertThat(pipeliteLauncher.getTaskCompletedCount()).isEqualTo(PROCESS_CNT);
    assertThat(pollCount.get()).isEqualTo(PROCESS_CNT);
    assertThat(executeCount.get()).isEqualTo(0);
  }

  public void testPollErrorExecuteSuccess() {
    init(ProcessExecutionState.ACTIVE, new PollErrorExecuteSuccessTaskExecutor());

    PipeliteLauncher pipeliteLauncher = pipeliteLauncher();
    ServerManager.run(pipeliteLauncher, pipeliteLauncher.serviceName());

    assertThat(pipeliteLauncher.getTaskFailedCount()).isEqualTo(0);
    assertThat(pipeliteLauncher.getTaskCompletedCount()).isEqualTo(PROCESS_CNT);
    assertThat(pollCount.get()).isEqualTo(PROCESS_CNT);
    assertThat(executeCount.get()).isEqualTo(PROCESS_CNT);
  }

  public void testPollErrorExecuteError() {
    init(ProcessExecutionState.ACTIVE, new PollErrorExecuteErrorTaskExecutor());

    PipeliteLauncher pipeliteLauncher = pipeliteLauncher();
    ServerManager.run(pipeliteLauncher, pipeliteLauncher.serviceName());

    assertThat(pipeliteLauncher.getTaskFailedCount()).isEqualTo(PROCESS_CNT);
    assertThat(pipeliteLauncher.getTaskCompletedCount()).isEqualTo(0);
    assertThat(pollCount.get()).isEqualTo(PROCESS_CNT);
    assertThat(executeCount.get()).isEqualTo(PROCESS_CNT);
  }

  public void testPollExceptionExecuteSuccess() {
    init(ProcessExecutionState.ACTIVE, new PollExceptionExecuteSuccessTaskExecutor());

    PipeliteLauncher pipeliteLauncher = pipeliteLauncher();
    ServerManager.run(pipeliteLauncher, pipeliteLauncher.serviceName());

    assertThat(pipeliteLauncher.getTaskFailedCount()).isEqualTo(0);
    assertThat(pipeliteLauncher.getTaskCompletedCount()).isEqualTo(PROCESS_CNT);
    assertThat(pollCount.get()).isEqualTo(PROCESS_CNT);
    assertThat(executeCount.get()).isEqualTo(PROCESS_CNT);
  }

  public void testPollExceptionExecuteError() {
    init(ProcessExecutionState.ACTIVE, new PollExceptionExecuteErrorTaskExecutor());

    PipeliteLauncher pipeliteLauncher = pipeliteLauncher();
    ServerManager.run(pipeliteLauncher, pipeliteLauncher.serviceName());

    assertThat(pipeliteLauncher.getTaskFailedCount()).isEqualTo(PROCESS_CNT);
    assertThat(pipeliteLauncher.getTaskCompletedCount()).isEqualTo(0);
    assertThat(pollCount.get()).isEqualTo(PROCESS_CNT);
    assertThat(executeCount.get()).isEqualTo(PROCESS_CNT);
  }
}
