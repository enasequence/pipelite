package pipelite.server;

import com.fasterxml.jackson.annotation.JsonCreator;
import lombok.AllArgsConstructor;
import lombok.Value;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.ObjectProvider;
import pipelite.UniqueStringGenerator;
import pipelite.entity.PipeliteMonitor;
import pipelite.executor.TaskExecutor;
import pipelite.monitor.MonitoredTaskExecutionResult;
import pipelite.monitor.TaskExecutionMonitor;
import pipelite.server.ServerManager;
import pipelite.server.TaskMonitor;
import pipelite.service.PipeliteMonitorService;
import pipelite.task.TaskExecutionResult;
import pipelite.task.TaskExecutionState;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;

@AllArgsConstructor
public class TaskMonitorTester {

  private final ObjectProvider<TaskMonitor> taskMonitorProvider;
  private final PipeliteMonitorService pipeliteMonitorService;
  private final String processName;

  private static final int MONITOR_COUNT = 100;
  private static final AtomicInteger successMonitorCount = new AtomicInteger();
  private static final AtomicInteger successDoneCount = new AtomicInteger();

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
}
