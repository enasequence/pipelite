package pipelite.server;

import lombok.AllArgsConstructor;
import lombok.Value;
import org.springframework.beans.factory.ObjectProvider;
import pipelite.UniqueStringGenerator;
import pipelite.entity.PipeliteMonitor;
import pipelite.monitor.MonitoredTaskExecutionResult;
import pipelite.monitor.TaskExecutionMonitor;
import pipelite.service.PipeliteMonitorService;
import pipelite.task.TaskExecutionResult;
import pipelite.task.TaskExecutionState;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

@AllArgsConstructor
public class TaskMonitorTester {

  private final ObjectProvider<TaskMonitor> taskMonitorProvider;
  private final PipeliteMonitorService pipeliteMonitorService;
  private final String processName;

  private static final int MONITOR_COUNT = 100;
  private static final AtomicInteger successMonitorCount = new AtomicInteger();
  private static final AtomicInteger successDoneCount = new AtomicInteger();
  private static final AtomicInteger permanentErrorMonitorCount = new AtomicInteger();
  private static final AtomicInteger permanentErrorDoneCount = new AtomicInteger();
  private static final AtomicInteger runtimeExceededMonitorCount = new AtomicInteger();
  private static final AtomicInteger runtimeExceededDoneCount = new AtomicInteger();
  private static final AtomicInteger lostMonitorCount = new AtomicInteger();
  private static final AtomicInteger lostDoneCount = new AtomicInteger();

  private TaskMonitor taskMonitor(
      Duration schedulerDelay, Duration maxRunDuration, Duration maxLostDuration) {
    TaskMonitor taskMonitor = taskMonitorProvider.getObject();
    taskMonitor.setShutdownPolicy(TaskMonitor.ShutdownPolicy.SHUTDOWN_IF_IDLE);
    taskMonitor.setSchedulerDelay(schedulerDelay);
    taskMonitor.setMaxRunDuration(maxRunDuration);
    taskMonitor.setMaxLostDuration(maxLostDuration);
    return taskMonitor;
  }

  @Value
  public static class SuccessTaskExecutionMonitor implements TaskExecutionMonitor {
    private final String processName;
    private final String processId;
    private final String taskName;

    @Override
    public MonitoredTaskExecutionResult monitor() {
      successMonitorCount.incrementAndGet();
      return MonitoredTaskExecutionResult.builder()
          .processName(processName)
          .processId(processId)
          .taskName(taskName)
          .taskExecutionState(TaskExecutionState.COMPLETED)
          .taskExecutionResult(TaskExecutionResult.defaultSuccess())
          .build();
    }

    @Override
    public void done(TaskExecutionResult taskExecutionResult) {
      if (taskExecutionResult.isSuccess()) {
        successDoneCount.incrementAndGet();
      }
    }
  }

  @Value
  public static class PermanentErrorTaskExecutionMonitor implements TaskExecutionMonitor {
    private final String processName;
    private final String processId;
    private final String taskName;

    @Override
    public MonitoredTaskExecutionResult monitor() {
      permanentErrorMonitorCount.incrementAndGet();
      return MonitoredTaskExecutionResult.builder()
          .processName(processName)
          .processId(processId)
          .taskName(taskName)
          .taskExecutionState(TaskExecutionState.FAILED)
          .taskExecutionResult(TaskExecutionResult.defaultPermanentError())
          .build();
    }

    @Override
    public void done(TaskExecutionResult taskExecutionResult) {
      if (taskExecutionResult.isPermanentError()) {
        permanentErrorDoneCount.incrementAndGet();
      }
    }
  }

  @Value
  public static class RuntimeExceededTaskExecutionMonitor implements TaskExecutionMonitor {
    private final String processName;
    private final String processId;
    private final String taskName;

    @Override
    public MonitoredTaskExecutionResult monitor() {
      runtimeExceededMonitorCount.incrementAndGet();
      return MonitoredTaskExecutionResult.builder()
          .processName(processName)
          .processId(processId)
          .taskName(taskName)
          .taskExecutionState(TaskExecutionState.ACTIVE)
          .build();
    }

    @Override
    public void done(TaskExecutionResult taskExecutionResult) {
      if (taskExecutionResult.isPermanentError()) {
        runtimeExceededDoneCount.incrementAndGet();
      }
    }
  }

  @Value
  public static class LostTaskExecutionMonitor implements TaskExecutionMonitor {
    private final String processName;
    private final String processId;
    private final String taskName;

    @Override
    public MonitoredTaskExecutionResult monitor() {
      lostMonitorCount.incrementAndGet();
      return MonitoredTaskExecutionResult.builder()
              .processName(processName)
              .processId(processId)
              .taskName(taskName)
              .taskExecutionState(TaskExecutionState.LOST)
              .build();
    }

    @Override
    public void done(TaskExecutionResult taskExecutionResult) {
      if (taskExecutionResult.isTransientError()) {
        lostDoneCount.incrementAndGet();
      }
    }
  }

  public void testSuccess() {
    TaskMonitor taskMonitor =
        taskMonitor(Duration.ofMillis(10), Duration.ofMillis(100), Duration.ofMillis(100));

    for (int i = 0; i < MONITOR_COUNT; ++i) {
      String processId = UniqueStringGenerator.randomProcessId();
      String taskName = UniqueStringGenerator.randomTaskName();
      TaskExecutionMonitor monitor =
          new SuccessTaskExecutionMonitor(processName, processId, taskName);
      PipeliteMonitor pipeliteMonitor = new PipeliteMonitor();
      pipeliteMonitor.setProcessName(processName);
      pipeliteMonitor.setProcessId(processId);
      pipeliteMonitor.setStageName(taskName);
      pipeliteMonitor.setStartTime(LocalDateTime.now());
      pipeliteMonitor.setActiveTime(LocalDateTime.now());
      pipeliteMonitor.setMonitorName(monitor.getClass().getName());
      pipeliteMonitor.setMonitorData(TaskExecutionMonitor.serialize(monitor));
      pipeliteMonitorService.saveMonitor(pipeliteMonitor);
    }

    ServerManager.run(taskMonitor, taskMonitor.serviceName());

    assertThat(taskMonitor.getTaskFailedCount()).isEqualTo(0);
    assertThat(taskMonitor.getTaskLostCount()).isEqualTo(0);
    assertThat(taskMonitor.getTaskRunTimeExceededCount()).isEqualTo(0);
    assertThat(taskMonitor.getTaskCompletedCount()).isEqualTo(MONITOR_COUNT);
    assertThat(successMonitorCount.get()).isEqualTo(MONITOR_COUNT);
    assertThat(successDoneCount.get()).isEqualTo(MONITOR_COUNT);
  }

  public void testPermanentError() {
    TaskMonitor taskMonitor =
        taskMonitor(Duration.ofMillis(10), Duration.ofMillis(100), Duration.ofMillis(100));

    for (int i = 0; i < MONITOR_COUNT; ++i) {
      String processId = UniqueStringGenerator.randomProcessId();
      String taskName = UniqueStringGenerator.randomTaskName();
      TaskExecutionMonitor monitor =
          new PermanentErrorTaskExecutionMonitor(processName, processId, taskName);
      PipeliteMonitor pipeliteMonitor = new PipeliteMonitor();
      pipeliteMonitor.setProcessName(processName);
      pipeliteMonitor.setProcessId(processId);
      pipeliteMonitor.setStageName(taskName);
      pipeliteMonitor.setStartTime(LocalDateTime.now());
      pipeliteMonitor.setActiveTime(LocalDateTime.now());
      pipeliteMonitor.setMonitorName(monitor.getClass().getName());
      pipeliteMonitor.setMonitorData(TaskExecutionMonitor.serialize(monitor));
      pipeliteMonitorService.saveMonitor(pipeliteMonitor);
    }

    ServerManager.run(taskMonitor, taskMonitor.serviceName());

    assertThat(taskMonitor.getTaskFailedCount()).isEqualTo(MONITOR_COUNT);
    assertThat(taskMonitor.getTaskLostCount()).isEqualTo(0);
    assertThat(taskMonitor.getTaskRunTimeExceededCount()).isEqualTo(0);
    assertThat(taskMonitor.getTaskCompletedCount()).isEqualTo(0);
    assertThat(permanentErrorMonitorCount.get()).isEqualTo(MONITOR_COUNT);
    assertThat(permanentErrorDoneCount.get()).isEqualTo(MONITOR_COUNT);
  }

  public void testRuntimeExceeded() {
    TaskMonitor taskMonitor =
        taskMonitor(Duration.ofMillis(10), Duration.ofSeconds(1), Duration.ofSeconds(10));

    for (int i = 0; i < MONITOR_COUNT; ++i) {
      String processId = UniqueStringGenerator.randomProcessId();
      String taskName = UniqueStringGenerator.randomTaskName();
      TaskExecutionMonitor monitor =
          new RuntimeExceededTaskExecutionMonitor(processName, processId, taskName);
      PipeliteMonitor pipeliteMonitor = new PipeliteMonitor();
      pipeliteMonitor.setProcessName(processName);
      pipeliteMonitor.setProcessId(processId);
      pipeliteMonitor.setStageName(taskName);
      pipeliteMonitor.setStartTime(LocalDateTime.now());
      pipeliteMonitor.setActiveTime(LocalDateTime.now());
      pipeliteMonitor.setMonitorName(monitor.getClass().getName());
      pipeliteMonitor.setMonitorData(TaskExecutionMonitor.serialize(monitor));
      pipeliteMonitorService.saveMonitor(pipeliteMonitor);
    }

    ServerManager.run(taskMonitor, taskMonitor.serviceName());

    assertThat(taskMonitor.getTaskFailedCount()).isEqualTo(0);
    assertThat(taskMonitor.getTaskLostCount()).isEqualTo(0);
    assertThat(taskMonitor.getTaskRunTimeExceededCount()).isEqualTo(MONITOR_COUNT);
    assertThat(taskMonitor.getTaskCompletedCount()).isEqualTo(0);
    assertThat(runtimeExceededMonitorCount.get()).isGreaterThanOrEqualTo(MONITOR_COUNT);
    assertThat(runtimeExceededDoneCount.get()).isEqualTo(MONITOR_COUNT);
  }

  public void testLost() {
    TaskMonitor taskMonitor =
            taskMonitor(Duration.ofMillis(10), Duration.ofSeconds(10), Duration.ofSeconds(1));

    for (int i = 0; i < MONITOR_COUNT; ++i) {
      String processId = UniqueStringGenerator.randomProcessId();
      String taskName = UniqueStringGenerator.randomTaskName();
      TaskExecutionMonitor monitor =
              new LostTaskExecutionMonitor(processName, processId, taskName);
      PipeliteMonitor pipeliteMonitor = new PipeliteMonitor();
      pipeliteMonitor.setProcessName(processName);
      pipeliteMonitor.setProcessId(processId);
      pipeliteMonitor.setStageName(taskName);
      pipeliteMonitor.setStartTime(LocalDateTime.now());
      pipeliteMonitor.setActiveTime(LocalDateTime.now());
      pipeliteMonitor.setMonitorName(monitor.getClass().getName());
      pipeliteMonitor.setMonitorData(TaskExecutionMonitor.serialize(monitor));
      pipeliteMonitorService.saveMonitor(pipeliteMonitor);
    }

    ServerManager.run(taskMonitor, taskMonitor.serviceName());

    assertThat(taskMonitor.getTaskFailedCount()).isEqualTo(0);
    assertThat(taskMonitor.getTaskLostCount()).isEqualTo(MONITOR_COUNT);
    assertThat(taskMonitor.getTaskRunTimeExceededCount()).isEqualTo(0);
    assertThat(taskMonitor.getTaskCompletedCount()).isEqualTo(0);
    assertThat(lostMonitorCount.get()).isGreaterThanOrEqualTo(MONITOR_COUNT);
    assertThat(lostDoneCount.get()).isEqualTo(MONITOR_COUNT);
  }
}
