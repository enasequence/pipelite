package pipelite.launcher;

import lombok.AllArgsConstructor;
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
import pipelite.resolver.ResultResolver;
import pipelite.service.PipeliteProcessService;
import pipelite.service.PipeliteStageService;
import pipelite.task.TaskExecutionResult;
import pipelite.task.TaskExecutionResultType;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

@AllArgsConstructor
public class PollablePipeliteLauncherTester {

  private final LauncherConfiguration launcherConfiguration;
  private final ProcessConfiguration processConfiguration;
  private final TaskConfiguration taskConfiguration;
  private final PipeliteProcessService pipeliteProcessService;
  private final PipeliteStageService pipeliteStageService;
  private final PipeliteLockService pipeliteLockService;

  private static final int WORKERS_CNT = 2;
  private static final int PROCESS_CNT = 10;
  private static final Duration DELAY_DURATION = Duration.ofMillis(100);

  private static final AtomicInteger successPollCount = new AtomicInteger();
  private static final AtomicInteger successExecuteCount = new AtomicInteger();
  private static final AtomicInteger permanentErrorPollCount = new AtomicInteger();
  private static final AtomicInteger permanentErrorExecuteCount = new AtomicInteger();
  private static final AtomicInteger transientErrorPollCount = new AtomicInteger();
  private static final AtomicInteger transientErrorExecuteCount = new AtomicInteger();
  private static final AtomicInteger exceptionPollCount = new AtomicInteger();
  private static final AtomicInteger exceptionExecuteCount = new AtomicInteger();

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
    pipeliteLauncher.setSchedulerDelay(DELAY_DURATION);
    return pipeliteLauncher;
  }

  @Value
  public static class SuccessTaskExecutor implements PollableExecutor {
    @Override
    public TaskExecutionResult execute(TaskInstance taskInstance) {
      successExecuteCount.incrementAndGet();
      return TaskExecutionResult.success();
    }

    @Override
    public TaskExecutionResult poll(TaskInstance taskInstance) {
      successPollCount.incrementAndGet();
      return TaskExecutionResult.success();
    }
  }

  @Value
  public static class PermanentErrorTaskExecutor implements PollableExecutor {
    @Override
    public TaskExecutionResult execute(TaskInstance taskInstance) {
      permanentErrorExecuteCount.incrementAndGet();
      return TaskExecutionResult.success();
    }

    @Override
    public TaskExecutionResult poll(TaskInstance taskInstance) {
      permanentErrorPollCount.incrementAndGet();
      return TaskExecutionResult.permanentError();
    }
  }

  @Value
  public static class TransientErrorTaskExecutor implements PollableExecutor {
    @Override
    public TaskExecutionResult execute(TaskInstance taskInstance) {
      transientErrorExecuteCount.incrementAndGet();
      return TaskExecutionResult.success();
    }

    @Override
    public TaskExecutionResult poll(TaskInstance taskInstance) {
      transientErrorPollCount.incrementAndGet();
      return TaskExecutionResult.transientError();
    }
  }

  @Value
  public static class ExceptionTaskExecutor implements PollableExecutor {
    @Override
    public TaskExecutionResult execute(TaskInstance taskInstance) {
      exceptionExecuteCount.incrementAndGet();
      return TaskExecutionResult.success();
    }

    @Override
    public TaskExecutionResult poll(TaskInstance taskInstance) {
      exceptionPollCount.incrementAndGet();
      throw new RuntimeException();
    }
  }

  private void init(ProcessExecutionState processExecutionState, TaskExecutor taskExecutor) {
    launcherConfiguration.setWorkers(WORKERS_CNT);

    List<ProcessInstance> processInstances = new ArrayList<>();

    for (int i = 0; i < PROCESS_CNT; ++i) {
      String processName = processConfiguration.getProcessName();
      String processId = UniqueStringGenerator.randomProcessId();
      String taskName = UniqueStringGenerator.randomTaskName();
      PipeliteProcess pipeliteProcess = PipeliteProcess.newExecution(processId, processName, 1);
      pipeliteProcess.setState(processExecutionState);
      pipeliteProcessService.saveProcess(pipeliteProcess);

      PipeliteStage pipeliteStage =
          PipeliteStage.newExecution(processId, processName, taskName, taskExecutor);
      pipeliteStage.setResultType(TaskExecutionResultType.ACTIVE);
      pipeliteStageService.saveStage(pipeliteStage);

      processInstances.add(
          new ProcessBuilder(processName, processId, 9)
              .task(taskName, taskExecutor, ResultResolver.DEFAULT_EXCEPTION_RESOLVER)
              .build());
    }

    TestInMemoryProcessFactory processFactory = new TestInMemoryProcessFactory(processInstances);
    processConfiguration.setProcessFactory(processFactory);
  }

  public void testSuccess() {
    init(ProcessExecutionState.ACTIVE, new SuccessTaskExecutor());

    PipeliteLauncher pipeliteLauncher = pipeliteLauncher();
    ServerManager.run(pipeliteLauncher, pipeliteLauncher.serviceName());

    assertThat(pipeliteLauncher.getTaskFailedCount()).isEqualTo(0);
    assertThat(pipeliteLauncher.getTaskSkippedCount()).isEqualTo(0);
    assertThat(pipeliteLauncher.getTaskCompletedCount()).isEqualTo(PROCESS_CNT);
    assertThat(pipeliteLauncher.getTaskRecoverCount()).isEqualTo(PROCESS_CNT);
    assertThat(pipeliteLauncher.getTaskRecoverFailedCount()).isEqualTo(0);
    assertThat(successPollCount.get()).isEqualTo(PROCESS_CNT);
    assertThat(successExecuteCount.get()).isEqualTo(0);
  }

  public void testPermanentError() {
    init(ProcessExecutionState.ACTIVE, new PermanentErrorTaskExecutor());

    PipeliteLauncher pipeliteLauncher = pipeliteLauncher();
    ServerManager.run(pipeliteLauncher, pipeliteLauncher.serviceName());

    assertThat(pipeliteLauncher.getTaskFailedCount()).isEqualTo(PROCESS_CNT);
    assertThat(pipeliteLauncher.getTaskSkippedCount()).isEqualTo(0);
    assertThat(pipeliteLauncher.getTaskCompletedCount()).isEqualTo(0);
    assertThat(pipeliteLauncher.getTaskRecoverCount()).isEqualTo(PROCESS_CNT);
    assertThat(pipeliteLauncher.getTaskRecoverFailedCount()).isEqualTo(0);
    assertThat(permanentErrorPollCount.get()).isEqualTo(PROCESS_CNT);
    assertThat(permanentErrorExecuteCount.get()).isEqualTo(0);
  }

  public void testTransientError() {
    init(ProcessExecutionState.ACTIVE, new TransientErrorTaskExecutor());

    PipeliteLauncher pipeliteLauncher = pipeliteLauncher();
    ServerManager.run(pipeliteLauncher, pipeliteLauncher.serviceName());

    assertThat(pipeliteLauncher.getTaskFailedCount()).isEqualTo(0);
    assertThat(pipeliteLauncher.getTaskSkippedCount()).isEqualTo(0);
    assertThat(pipeliteLauncher.getTaskCompletedCount()).isEqualTo(PROCESS_CNT);
    assertThat(pipeliteLauncher.getTaskRecoverCount()).isEqualTo(PROCESS_CNT);
    assertThat(pipeliteLauncher.getTaskRecoverFailedCount()).isEqualTo(0);
    assertThat(transientErrorPollCount.get()).isEqualTo(PROCESS_CNT);
    assertThat(transientErrorExecuteCount.get()).isEqualTo(PROCESS_CNT);
  }

  public void testException() {
    init(ProcessExecutionState.ACTIVE, new ExceptionTaskExecutor());

    PipeliteLauncher pipeliteLauncher = pipeliteLauncher();
    ServerManager.run(pipeliteLauncher, pipeliteLauncher.serviceName());

    assertThat(pipeliteLauncher.getTaskFailedCount()).isEqualTo(0);
    assertThat(pipeliteLauncher.getTaskSkippedCount()).isEqualTo(0);
    assertThat(pipeliteLauncher.getTaskCompletedCount()).isEqualTo(PROCESS_CNT);
    assertThat(pipeliteLauncher.getTaskRecoverCount()).isEqualTo(0);
    assertThat(pipeliteLauncher.getTaskRecoverFailedCount()).isEqualTo(PROCESS_CNT);
    assertThat(exceptionPollCount.get()).isEqualTo(PROCESS_CNT);
    assertThat(exceptionExecuteCount.get()).isEqualTo(PROCESS_CNT);
  }
}
