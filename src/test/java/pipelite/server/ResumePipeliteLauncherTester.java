package pipelite.server;

import lombok.AllArgsConstructor;
import lombok.Value;
import org.springframework.beans.factory.ObjectProvider;
import pipelite.TestInMemoryProcessFactory;
import pipelite.UniqueStringGenerator;
import pipelite.configuration.LauncherConfiguration;
import pipelite.configuration.ProcessConfiguration;
import pipelite.entity.PipeliteProcess;
import pipelite.entity.PipeliteStage;
import pipelite.executor.TaskExecutor;
import pipelite.process.ProcessInstance;
import pipelite.process.ProcessBuilder;
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
public class ResumePipeliteLauncherTester {

  private final LauncherConfiguration launcherConfiguration;
  private final ProcessConfiguration processConfiguration;
  private final ObjectProvider<PipeliteLauncher> pipeliteLauncherObjectProvider;
  private final PipeliteProcessService pipeliteProcessService;
  private final PipeliteStageService pipeliteStageService;

  private static final int WORKERS_CNT = 2;
  private static final int PROCESS_CNT = 10;
  private static final Duration DELAY_DURATION = Duration.ofMillis(100);

  private static final AtomicInteger successResumeCount = new AtomicInteger();
  private static final AtomicInteger successExecuteCount = new AtomicInteger();
  private static final AtomicInteger permanentErrorResumeCount = new AtomicInteger();
  private static final AtomicInteger permanentErrorExecuteCount = new AtomicInteger();
  private static final AtomicInteger transientErrorResumeCount = new AtomicInteger();
  private static final AtomicInteger transientErrorExecuteCount = new AtomicInteger();
  private static final AtomicInteger exceptionResumeCount = new AtomicInteger();
  private static final AtomicInteger exceptionExecuteCount = new AtomicInteger();
  private static final AtomicInteger newProcessResumeCount = new AtomicInteger();
  private static final AtomicInteger newProcessExecuteCount = new AtomicInteger();

  private PipeliteLauncher pipeliteLauncher() {
    PipeliteLauncher pipeliteLauncher = pipeliteLauncherObjectProvider.getObject();
    pipeliteLauncher.setShutdownPolicy(ShutdownPolicy.SHUTDOWN_IF_IDLE);
    pipeliteLauncher.setSchedulerDelay(DELAY_DURATION);
    return pipeliteLauncher;
  }

  @Value
  public static class SuccessTaskExecutor implements TaskExecutor {
    @Override
    public TaskExecutionResult execute(TaskInstance taskInstance) {
      successExecuteCount.incrementAndGet();
      return TaskExecutionResult.defaultSuccess();
    }

    @Override
    public TaskExecutionResult resume(TaskInstance taskInstance) {
      successResumeCount.incrementAndGet();
      return TaskExecutionResult.defaultSuccess();
    }
  }

  @Value
  public static class PermanentErrorTaskExecutor implements TaskExecutor {
    @Override
    public TaskExecutionResult execute(TaskInstance taskInstance) {
      permanentErrorExecuteCount.incrementAndGet();
      return TaskExecutionResult.defaultSuccess();
    }

    @Override
    public TaskExecutionResult resume(TaskInstance taskInstance) {
      permanentErrorResumeCount.incrementAndGet();
      return TaskExecutionResult.defaultPermanentError();
    }
  }

  @Value
  public static class TransientErrorTaskExecutor implements TaskExecutor {
    @Override
    public TaskExecutionResult execute(TaskInstance taskInstance) {
      transientErrorExecuteCount.incrementAndGet();
      return TaskExecutionResult.defaultSuccess();
    }

    @Override
    public TaskExecutionResult resume(TaskInstance taskInstance) {
      transientErrorResumeCount.incrementAndGet();
      return TaskExecutionResult.defaultTransientError();
    }
  }

  @Value
  public static class ExceptionTaskExecutor implements TaskExecutor {
    @Override
    public TaskExecutionResult execute(TaskInstance taskInstance) {
      exceptionExecuteCount.incrementAndGet();
      return TaskExecutionResult.defaultSuccess();
    }

    @Override
    public TaskExecutionResult resume(TaskInstance taskInstance) {
      exceptionResumeCount.incrementAndGet();
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
    assertThat(successResumeCount.get()).isEqualTo(PROCESS_CNT);
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
    assertThat(permanentErrorResumeCount.get()).isEqualTo(PROCESS_CNT);
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
    assertThat(transientErrorResumeCount.get()).isEqualTo(PROCESS_CNT);
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
    assertThat(exceptionResumeCount.get()).isEqualTo(PROCESS_CNT);
    assertThat(exceptionExecuteCount.get()).isEqualTo(PROCESS_CNT);
  }
}
