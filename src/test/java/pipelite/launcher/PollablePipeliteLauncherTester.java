package pipelite.launcher;

import lombok.Value;
import pipelite.TestInMemoryProcessFactory;
import pipelite.UniqueStringGenerator;
import pipelite.configuration.LauncherConfiguration;
import pipelite.configuration.ProcessConfiguration;
import pipelite.configuration.TaskConfiguration;
import pipelite.entity.PipeliteProcess;
import pipelite.entity.PipeliteStage;
import pipelite.executor.PollableExecutor;
import pipelite.executor.TaskExecutor;
import pipelite.process.ProcessInstance;
import pipelite.process.ProcessBuilder;
import pipelite.service.PipeliteLockService;
import pipelite.task.TaskInstance;
import pipelite.process.ProcessExecutionState;
import pipelite.service.PipeliteProcessService;
import pipelite.service.PipeliteStageService;
import pipelite.task.TaskExecutionResult;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

public class PollablePipeliteLauncherTester {

  private final LauncherConfiguration launcherConfiguration;
  private final ProcessConfiguration processConfiguration;
  private final TaskConfiguration taskConfiguration;
  private final PipeliteProcessService pipeliteProcessService;
  private final PipeliteStageService pipeliteStageService;
  private final PipeliteLockService pipeliteLockService;

  public PollablePipeliteLauncherTester(
      LauncherConfiguration launcherConfiguration,
      ProcessConfiguration processConfiguration,
      TaskConfiguration taskConfiguration,
      PipeliteProcessService pipeliteProcessService,
      PipeliteStageService pipeliteStageService,
      PipeliteLockService pipeliteLockService) {
    this.launcherConfiguration = launcherConfiguration;
    this.processConfiguration = processConfiguration;
    this.taskConfiguration = taskConfiguration;
    this.pipeliteProcessService = pipeliteProcessService;
    this.pipeliteStageService = pipeliteStageService;
    this.pipeliteLockService = pipeliteLockService;

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
            pipeliteProcessService,
            pipeliteStageService,
            pipeliteLockService);
    pipeliteLauncher.setShutdownPolicy(ShutdownPolicy.SHUTDOWN_IF_IDLE);
    return pipeliteLauncher;
  }

  @Value
  public static class PollSuccessExecuteSuccessTaskExecutor implements PollableExecutor {
    @Override
    public TaskExecutionResult execute(TaskInstance taskInstance) {
      executeCount.incrementAndGet();
      return TaskExecutionResult.success();
    }

    @Override
    public TaskExecutionResult poll(TaskInstance taskInstance) {
      pollCount.incrementAndGet();
      return TaskExecutionResult.success();
    }
  }

  @Value
  public static class PollErrorExecuteSuccessTaskExecutor implements PollableExecutor {
    @Override
    public TaskExecutionResult execute(TaskInstance taskInstance) {
      executeCount.incrementAndGet();
      return TaskExecutionResult.success();
    }

    @Override
    public TaskExecutionResult poll(TaskInstance taskInstance) {
      pollCount.incrementAndGet();
      return TaskExecutionResult.error();
    }
  }

  @Value
  public static class PollErrorExecuteErrorTaskExecutor implements PollableExecutor {
    @Override
    public TaskExecutionResult execute(TaskInstance taskInstance) {
      executeCount.incrementAndGet();
      return TaskExecutionResult.error();
    }

    @Override
    public TaskExecutionResult poll(TaskInstance taskInstance) {
      pollCount.incrementAndGet();
      return TaskExecutionResult.error();
    }
  }

  @Value
  public static class PollExceptionExecuteSuccessTaskExecutor implements PollableExecutor {
    @Override
    public TaskExecutionResult execute(TaskInstance taskInstance) {
      executeCount.incrementAndGet();
      return TaskExecutionResult.success();
    }

    @Override
    public TaskExecutionResult poll(TaskInstance taskInstance) {
      pollCount.incrementAndGet();
      throw new RuntimeException();
    }
  }

  @Value
  public static class PollExceptionExecuteErrorTaskExecutor implements PollableExecutor {
    @Override
    public TaskExecutionResult execute(TaskInstance taskInstance) {
      executeCount.incrementAndGet();
      return TaskExecutionResult.error();
    }

    @Override
    public TaskExecutionResult poll(TaskInstance taskInstance) {
      pollCount.incrementAndGet();
      throw new RuntimeException();
    }
  }

  private void init(ProcessExecutionState processExecutionState, TaskExecutor taskExecutor) {
    List<ProcessInstance> processInstances = new ArrayList<>();

    for (int i = 0; i < PROCESS_CNT; ++i) {
      String processName = processConfiguration.getProcessName();
      String processId = UniqueStringGenerator.randomProcessId();
      String taskName = UniqueStringGenerator.randomTaskName();
      PipeliteProcess pipeliteProcess = PipeliteProcess.newExecution(processId, processName, 1);
      pipeliteProcess.setState(processExecutionState);
      pipeliteProcessService.saveProcess(pipeliteProcess);

      ProcessInstance processInstance =
          new ProcessBuilder(processName, processId, 9).task(taskName, taskExecutor).build();
      processInstances.add(processInstance);

      TaskInstance taskInstance = processInstance.getTasks().get(0);
      PipeliteStage pipeliteStage = PipeliteStage.createExecution(taskInstance);
      pipeliteStage.startExecution(taskInstance);
      pipeliteStageService.saveStage(pipeliteStage);
    }

    TestInMemoryProcessFactory processFactory = new TestInMemoryProcessFactory(processInstances);
    processConfiguration.setProcessFactory(processFactory);
  }

  public void testPollSuccessExecuteSuccess() {
    init(ProcessExecutionState.ACTIVE, new PollSuccessExecuteSuccessTaskExecutor());

    PipeliteLauncher pipeliteLauncher = pipeliteLauncher();
    ServerManager.run(pipeliteLauncher, pipeliteLauncher.serviceName());

    assertThat(pipeliteLauncher.getTaskFailedCount()).isEqualTo(0);
    assertThat(pipeliteLauncher.getTaskSkippedCount()).isEqualTo(0);
    assertThat(pipeliteLauncher.getTaskCompletedCount()).isEqualTo(PROCESS_CNT);
    assertThat(pollCount.get()).isEqualTo(PROCESS_CNT);
    assertThat(executeCount.get()).isEqualTo(0);
  }

  public void testPollErrorExecuteSuccess() {
    init(ProcessExecutionState.ACTIVE, new PollErrorExecuteSuccessTaskExecutor());

    PipeliteLauncher pipeliteLauncher = pipeliteLauncher();
    ServerManager.run(pipeliteLauncher, pipeliteLauncher.serviceName());

    assertThat(pipeliteLauncher.getTaskFailedCount()).isEqualTo(0);
    assertThat(pipeliteLauncher.getTaskSkippedCount()).isEqualTo(0);
    assertThat(pipeliteLauncher.getTaskCompletedCount()).isEqualTo(PROCESS_CNT);
    assertThat(pollCount.get()).isEqualTo(PROCESS_CNT);
    assertThat(executeCount.get()).isEqualTo(PROCESS_CNT);
  }

  public void testPollErrorExecuteError() {
    init(ProcessExecutionState.ACTIVE, new PollErrorExecuteErrorTaskExecutor());

    PipeliteLauncher pipeliteLauncher = pipeliteLauncher();
    ServerManager.run(pipeliteLauncher, pipeliteLauncher.serviceName());

    assertThat(pipeliteLauncher.getTaskFailedCount()).isEqualTo(PROCESS_CNT);
    assertThat(pipeliteLauncher.getTaskSkippedCount()).isEqualTo(0);
    assertThat(pipeliteLauncher.getTaskCompletedCount()).isEqualTo(0);
    assertThat(pollCount.get()).isEqualTo(PROCESS_CNT);
    assertThat(executeCount.get()).isEqualTo(PROCESS_CNT);
  }

  public void testPollExceptionExecuteSuccess() {
    init(ProcessExecutionState.ACTIVE, new PollExceptionExecuteSuccessTaskExecutor());

    PipeliteLauncher pipeliteLauncher = pipeliteLauncher();
    ServerManager.run(pipeliteLauncher, pipeliteLauncher.serviceName());

    assertThat(pipeliteLauncher.getTaskFailedCount()).isEqualTo(0);
    assertThat(pipeliteLauncher.getTaskSkippedCount()).isEqualTo(0);
    assertThat(pipeliteLauncher.getTaskCompletedCount()).isEqualTo(PROCESS_CNT);
    assertThat(pollCount.get()).isEqualTo(PROCESS_CNT);
    assertThat(executeCount.get()).isEqualTo(PROCESS_CNT);
  }

  public void testPollExceptionExecuteError() {
    init(ProcessExecutionState.ACTIVE, new PollExceptionExecuteErrorTaskExecutor());

    PipeliteLauncher pipeliteLauncher = pipeliteLauncher();
    ServerManager.run(pipeliteLauncher, pipeliteLauncher.serviceName());

    assertThat(pipeliteLauncher.getTaskFailedCount()).isEqualTo(PROCESS_CNT);
    assertThat(pipeliteLauncher.getTaskSkippedCount()).isEqualTo(0);
    assertThat(pipeliteLauncher.getTaskCompletedCount()).isEqualTo(0);
    assertThat(pollCount.get()).isEqualTo(PROCESS_CNT);
    assertThat(executeCount.get()).isEqualTo(PROCESS_CNT);
  }
}
