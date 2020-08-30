/*
 * Copyright 2018-2019 EMBL - European Bioinformatics Institute
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package pipelite.server;

import com.google.common.util.concurrent.AbstractScheduledService;
import lombok.extern.flogger.Flogger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;
import pipelite.configuration.LauncherConfiguration;
import pipelite.configuration.ProcessConfiguration;
import pipelite.entity.PipeliteMonitor;
import pipelite.log.LogKey;
import pipelite.monitor.MonitoredTaskExecutionResult;
import pipelite.monitor.TaskExecutionMonitor;
import pipelite.service.PipeliteLockService;
import pipelite.service.PipeliteMonitorService;
import pipelite.service.PipeliteProcessService;
import pipelite.task.TaskExecutionResult;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Flogger
@Component()
@Scope("prototype")
public class TaskMonitor extends AbstractScheduledService {

  private final LauncherConfiguration launcherConfiguration;
  private final ProcessConfiguration processConfiguration;
  private final PipeliteProcessService pipeliteProcessService;
  private final PipeliteMonitorService pipeliteMonitorService;
  private final PipeliteLockService pipeliteLockService;

  private class MonitorStatus {
    public String processName;
    public String processId;
    public String taskName;
    public LocalDateTime startTime = LocalDateTime.now();
    public LocalDateTime activeTime;
    public LocalDateTime monitorTime;
    public boolean kill;
    public boolean done;
    public boolean lock; // Monitor recovered and locked by task monitor.
  }

  private ShutdownPolicy shutdownPolicy = ShutdownPolicy.WAIT_IF_IDLE;
  private Duration schedulerDelay = Duration.ofSeconds(10);
  private Duration maxLostDuration = Duration.ofHours(1);
  private Duration maxRunDuration = Duration.ofDays(7);
  private boolean failInvalidMonitorData = false;

  private Map<TaskExecutionMonitor, MonitorStatus> monitors = new ConcurrentHashMap<>();

  private int taskFailedCount = 0;
  private int taskRunTimeExceededCount = 0;
  private int taskLostCount = 0;
  private int taskCompletedCount = 0;

  public TaskMonitor(
      @Autowired LauncherConfiguration launcherConfiguration,
      @Autowired ProcessConfiguration processConfiguration,
      @Autowired PipeliteProcessService pipeliteProcessService,
      @Autowired PipeliteMonitorService pipeliteMonitorService,
      @Autowired PipeliteLockService pipeliteLockService) {
    this.launcherConfiguration = launcherConfiguration;
    this.pipeliteMonitorService = pipeliteMonitorService;
    this.pipeliteProcessService = pipeliteProcessService;
    this.processConfiguration = processConfiguration;
    this.pipeliteLockService = pipeliteLockService;
  }

  public void monitor(TaskExecutionMonitor monitor) {
    monitors.put(monitor, new MonitorStatus());
  }

  public void kill(TaskExecutionMonitor monitor) {
    monitors.get(monitor).kill = true;
  }

  @Override
  public String serviceName() {
    return this.getClass().getSimpleName();
  }

  @Override
  protected void startUp() {
    String processName = processConfiguration.getProcessName();
    for (PipeliteMonitor pipeliteMonitor : pipeliteMonitorService.getActiveMonitors(processName)) {
      String processId = pipeliteMonitor.getProcessId();

      TaskExecutionMonitor monitor;

      try {
        monitor =
            TaskExecutionMonitor.deserialize(
                pipeliteMonitor.getMonitorName(), pipeliteMonitor.getMonitorData());

      } catch (Exception ex) {
        if (failInvalidMonitorData) {
          throw new RuntimeException("Failed to deserialize monitor data.", ex);
        } else {
          log.atWarning()
              .with(LogKey.PROCESS_NAME, processName)
              .with(LogKey.PROCESS_ID, processId)
              .with(LogKey.TASK_NAME, pipeliteMonitor.getStageName())
              .log("Failed to deserialize monitor data. Removing monitor data.");
          pipeliteMonitorService.delete(pipeliteMonitor);
          continue;
        }
      }

      MonitorStatus monitorStatus = new MonitorStatus();
      monitorStatus.processName = processName;
      monitorStatus.processId = processId;
      monitorStatus.taskName = pipeliteMonitor.getStageName();
      monitorStatus.startTime = pipeliteMonitor.getStartTime();
      monitorStatus.activeTime = pipeliteMonitor.getActiveTime();
      monitorStatus.monitorTime = pipeliteMonitor.getMonitorTime();

      if (!pipeliteLockService.isProcessLocked(processName, processId)) {
        if (pipeliteLockService.lockProcess(
            launcherConfiguration.getLauncherName(), processName, processId)) {
          monitorStatus.lock = true;
          monitors.put(monitor, monitorStatus);
        }
      }
    }
  }

  @Override
  protected Scheduler scheduler() {
    return Scheduler.newFixedDelaySchedule(Duration.ZERO, schedulerDelay);
  }

  @Override
  protected void runOneIteration() {
    if (!isRunning()) {
      return;
    }

    monitors.forEach(
        (monitor, monitorStatus) -> {
          monitorTask(monitor, monitorStatus);
        });

    monitors.forEach(
        (monitor, monitorStatus) -> {
          if (monitorStatus.kill || monitorStatus.done) {
            monitors.remove(monitor);
          }
        });

    stopIfIdle();
  }

  private void stopIfIdle() {
    if (monitors.isEmpty() && ShutdownPolicy.SHUTDOWN_IF_IDLE.equals(shutdownPolicy)) {
      log.atInfo()
          .with(LogKey.LAUNCHER_NAME, launcherConfiguration.getLauncherName())
          .with(LogKey.PROCESS_NAME, processConfiguration.getProcessName())
          .log("Shutting down inactive task monitor.");
      stopAsync();
    }
  }

  private void monitorTask(TaskExecutionMonitor monitor, MonitorStatus monitorStatus) {
    monitorStatus.monitorTime = LocalDateTime.now();

    String processName = monitorStatus.processName;
    String processId = monitorStatus.processId;
    String taskName = monitorStatus.taskName;

    MonitoredTaskExecutionResult result = monitor.monitor();

    switch (result.getTaskExecutionState()) {
      case ACTIVE:
        if (monitorStatus.startTime.plus(maxRunDuration).isBefore(LocalDateTime.now())) {
          log.atSevere()
              .with(LogKey.PROCESS_NAME, processName)
              .with(LogKey.PROCESS_ID, processId)
              .with(LogKey.TASK_NAME, taskName)
              .log(
                  "Monitored task execution run time exceeded. Considering task permanently failed.");
          ++taskRunTimeExceededCount;
          done(monitor, monitorStatus, TaskExecutionResult.runtimeExceededPermanentError());
        } else {
          save(monitor, monitorStatus);
        }
        break;

      case LOST:
        if (monitorStatus.activeTime == null
                && monitorStatus.startTime.plus(maxLostDuration).isBefore(LocalDateTime.now())
            || monitorStatus.activeTime.plus(maxLostDuration).isBefore(LocalDateTime.now())) {
          log.atSevere()
              .with(LogKey.PROCESS_NAME, processName)
              .with(LogKey.PROCESS_ID, processId)
              .with(LogKey.TASK_NAME, taskName)
              .log("Monitored task lost time exceeded. Considering task permanently failed.");
          ++taskLostCount;
          done(monitor, monitorStatus, TaskExecutionResult.lostTaskTransientError());
        }
        break;

      case COMPLETED:
        ++taskCompletedCount;
        done(monitor, monitorStatus, result.getTaskExecutionResult());
        break;
      case FAILED:
        ++taskFailedCount;
        done(monitor, monitorStatus, result.getTaskExecutionResult());
        break;
    }
  }

  @Override
  protected void shutDown() {
    monitors.forEach((monitor, monitorStatus) -> unlock(monitorStatus));
  }

  private void unlock(MonitorStatus monitorStatus) {
    String processName = monitorStatus.processName;
    String processId = monitorStatus.processId;
    if (monitorStatus.lock) {
      pipeliteLockService.unlockProcess(
          launcherConfiguration.getLauncherName(), processName, processId);
    }
  }

  private PipeliteMonitor get(TaskExecutionMonitor monitor, MonitorStatus monitorStatus) {
    PipeliteMonitor pipeliteMonitor = new PipeliteMonitor();
    pipeliteMonitor.setProcessName(monitorStatus.processName);
    pipeliteMonitor.setProcessId(monitorStatus.processId);
    pipeliteMonitor.setStageName(monitorStatus.taskName);
    pipeliteMonitor.setStartTime(monitorStatus.startTime);
    pipeliteMonitor.setActiveTime(monitorStatus.activeTime);
    pipeliteMonitor.setMonitorTime(monitorStatus.monitorTime);
    pipeliteMonitor.setMonitorName(monitor.getClass().getName());
    try {
      pipeliteMonitor.setMonitorData(TaskExecutionMonitor.serialize(monitor));
    } catch (Exception ex) {
      log.atSevere()
          .withCause(ex)
          .with(LogKey.PROCESS_NAME, monitorStatus.processName)
          .with(LogKey.PROCESS_ID, monitorStatus.processId)
          .with(LogKey.TASK_NAME, monitorStatus.taskName)
          .log("Failed to serialize monitor data.");
      if (failInvalidMonitorData) {
        throw new RuntimeException("Failed to serialize monitor data.", ex);
      }
    }
    return pipeliteMonitor;
  }

  private void save(TaskExecutionMonitor monitor, MonitorStatus monitorStatus) {
    monitorStatus.activeTime = LocalDateTime.now();
    pipeliteMonitorService.saveMonitor(get(monitor, monitorStatus));
  }

  private void done(
      TaskExecutionMonitor monitor, MonitorStatus monitorStatus, TaskExecutionResult result) {
    monitor.done(result);
    unlock(monitorStatus);
    monitorStatus.done = true;
    pipeliteMonitorService.delete(get(monitor, monitorStatus));
  }

  public boolean isFailInvalidMonitorData() {
    return failInvalidMonitorData;
  }

  public void setFailInvalidMonitorData(boolean failInvalidMonitorData) {
    this.failInvalidMonitorData = failInvalidMonitorData;
  }

  public int getTaskFailedCount() {
    return taskFailedCount;
  }

  public int getTaskRunTimeExceededCount() {
    return taskRunTimeExceededCount;
  }

  public int getTaskLostCount() {
    return taskLostCount;
  }

  public int getTaskCompletedCount() {
    return taskCompletedCount;
  }

  public Duration getSchedulerDelay() {
    return schedulerDelay;
  }

  public void setSchedulerDelay(Duration schedulerDelay) {
    this.schedulerDelay = schedulerDelay;
  }

  public Duration getMaxLostDuration() {
    return maxLostDuration;
  }

  public void setMaxLostDuration(Duration maxLostDuration) {
    this.maxLostDuration = maxLostDuration;
  }

  public Duration getMaxRunDuration() {
    return maxRunDuration;
  }

  public void setMaxRunDuration(Duration maxRunDuration) {
    this.maxRunDuration = maxRunDuration;
  }

  public ShutdownPolicy getShutdownPolicy() {
    return shutdownPolicy;
  }

  public void setShutdownPolicy(ShutdownPolicy shutdownPolicy) {
    this.shutdownPolicy = shutdownPolicy;
  }
}
