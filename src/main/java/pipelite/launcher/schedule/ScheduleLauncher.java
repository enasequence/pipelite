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
package pipelite.launcher.schedule;

import com.google.common.flogger.FluentLogger;
import com.google.common.util.concurrent.AbstractScheduledService;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.Data;
import lombok.extern.flogger.Flogger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import pipelite.configuration.LauncherConfiguration;
import pipelite.configuration.TaskConfiguration;
import pipelite.cron.CronUtils;
import pipelite.entity.ProcessEntity;
import pipelite.entity.ScheduleEntity;
import pipelite.launcher.ServerManager;
import pipelite.launcher.pipelite.Locker;
import pipelite.launcher.pipelite.PipeliteLauncher;
import pipelite.launcher.process.ProcessLauncher;
import pipelite.log.LogKey;
import pipelite.process.Process;
import pipelite.process.ProcessFactory;
import pipelite.service.LockService;
import pipelite.service.ProcessService;
import pipelite.service.ScheduleService;
import pipelite.service.TaskService;

@Flogger
@Component
public class ScheduleLauncher extends AbstractScheduledService {

  private final LauncherConfiguration launcherConfiguration;
  private final TaskConfiguration taskConfiguration;
  private final ScheduleService scheduleService;
  private final ProcessService processService;
  private final TaskService taskService;
  private final LockService lockService;
  private final String launcherName;
  private final Locker locker;
  private final ExecutorService executorService;
  private final int workers;

  private final AtomicInteger processFailedToCreateCount = new AtomicInteger(0);
  private final AtomicInteger processFailedToExecuteCount = new AtomicInteger(0);
  private final AtomicInteger processCompletedCount = new AtomicInteger(0);
  private final AtomicInteger taskFailedCount = new AtomicInteger(0);
  private final AtomicInteger taskCompletedCount = new AtomicInteger(0);

  private final Map<String, Schedule> activeProcesses = new ConcurrentHashMap<>();

  @Data
  private static class Schedule {
    private ScheduleEntity scheduleEntity;
    private ProcessEntity processEntity;
    private Process process;
    private LocalDateTime launchTime;
  }

  private final ArrayList<Schedule> schedules = new ArrayList<>();
  private LocalDateTime schedulesValidUntil = LocalDateTime.now();

  private long iterations = 0;
  private Long maxIterations;

  public static final Duration DEFAULT_PROCESS_SCHEDULING_FREQUENCY = Duration.ofMinutes(5);
  private final Duration processSchedulingFrequency;

  public ScheduleLauncher(
      @Autowired LauncherConfiguration launcherConfiguration,
      @Autowired TaskConfiguration taskConfiguration,
      @Autowired ScheduleService scheduleService,
      @Autowired ProcessService processService,
      @Autowired TaskService taskService,
      @Autowired LockService lockService) {
    this.launcherConfiguration = launcherConfiguration;
    this.taskConfiguration = taskConfiguration;
    this.scheduleService = scheduleService;
    this.processService = processService;
    this.taskService = taskService;
    this.lockService = lockService;
    this.launcherName = launcherConfiguration.getLauncherName();
    this.locker = new Locker(launcherName, lockService);
    this.workers =
        launcherConfiguration.getWorkers() > 0
            ? launcherConfiguration.getWorkers()
            : PipeliteLauncher.DEFAULT_WORKERS;
    this.executorService = Executors.newFixedThreadPool(workers);

    if (launcherConfiguration.getProcessLaunchFrequency() != null) {
      this.processSchedulingFrequency = launcherConfiguration.getProcessSchedulingFrequency();
    } else {
      this.processSchedulingFrequency = DEFAULT_PROCESS_SCHEDULING_FREQUENCY;
    }
  }

  @Override
  public String serviceName() {
    return launcherName;
  }

  @Override
  protected void startUp() {
    logContext(log.atInfo()).log("Starting up launcher");
    if (!locker.lockLauncher()) {
      throw new RuntimeException("Could not start launcher");
    }
  }

  @Override
  protected Scheduler scheduler() {
    return Scheduler.newFixedDelaySchedule(Duration.ZERO, processSchedulingFrequency);
  }

  @Override
  protected void runOneIteration() {
    if (!isRunning()) {
      return;
    }

    logContext(log.atInfo()).log("Running launcher");

    if (schedules.isEmpty() || schedulesValidUntil.isBefore(LocalDateTime.now())) {
      scheduleProcesses();
    }

    for (Schedule schedule : schedules) {
      if (!activeProcesses.containsKey(schedule.processEntity.getProcessName())
          && schedule.launchTime.isAfter(LocalDateTime.now())) {
        if (createProcess(schedule)) {
          launchProcess(schedule);
        }
      }
    }

    stopIfMaxIterations();
  }

  private void stopIfMaxIterations() {
    if (maxIterations != null && ++iterations > maxIterations) {
      stopAsync();
    }
  }

  private void scheduleProcesses() {

    logContext(log.atInfo()).log("Scheduling processes");

    schedules.clear();
    for (ScheduleEntity scheduleEntity : scheduleService.getAllProcessSchedules(launcherName)) {

      String scheduleDescription = "invalid cron expression";
      if (CronUtils.validate(scheduleEntity.getSchedule())) {
        Schedule schedule = new Schedule();
        schedule.setScheduleEntity(scheduleEntity);
        schedule.setLaunchTime(CronUtils.launchTime(scheduleEntity.getSchedule()));
        schedules.add(schedule);
        scheduleDescription = CronUtils.describe(scheduleEntity.getSchedule());
      }
      if (!scheduleDescription.equals(scheduleEntity.getDescription())) {
        scheduleEntity.setDescription(scheduleDescription);
        scheduleService.saveProcessSchedule(scheduleEntity);
      }
    }
  }

  private boolean createProcess(Schedule schedule) {

    String processName = schedule.getScheduleEntity().getProcessName();
    String processId = String.valueOf(schedule.getScheduleEntity().getExecutionCount() + 1);

    logContext(log.atInfo(), processName, processId).log("Creating new scheduled process instance");

    ProcessFactory processFactory =
        ProcessFactory.getProcessFactory(schedule.getScheduleEntity().getProcessFactoryName());

    Process process = processFactory.create(processId);

    if (process == null) {
      logContext(log.atSevere(), processName, processId).log("Could not create process instance");
      processFailedToCreateCount.incrementAndGet();
      return false;
    }

    if (!validateProcess(schedule, process)) {
      logContext(log.atSevere(), processName, processId).log("Failed to validate process instance");
      processFailedToCreateCount.incrementAndGet();
      return false;
    }

    Optional<ProcessEntity> savedProcessEntity =
        processService.getSavedProcess(processName, processId);

    if (savedProcessEntity.isPresent()) {
      logContext(log.atSevere(), processName, processId)
          .log("Could not create process instance because process id already exists");
      processFailedToCreateCount.incrementAndGet();
      return false;
    }

    ProcessEntity newProcessEntity = ProcessEntity.newExecution(processId, processName, 9);
    processService.saveProcess(newProcessEntity);

    schedule.setProcess(process);
    schedule.setProcessEntity(newProcessEntity);

    schedule.getScheduleEntity().startExecution();
    scheduleService.saveProcessSchedule(schedule.getScheduleEntity());

    return true;
  }

  private void launchProcess(Schedule schedule) {
    Process process = schedule.getProcess();
    ProcessEntity processEntity = schedule.getProcessEntity();
    String processName = processEntity.getProcessName();
    String processId = processEntity.getProcessId();

    logContext(log.atInfo(), processName, processId).log("Launching process");

    ProcessLauncher processLauncher =
        new ProcessLauncher(launcherConfiguration, taskConfiguration, processService, taskService);

    processLauncher.init(process, processEntity);

    executorService.execute(
        () -> {
          activeProcesses.put(processId, schedule);
          try {
            if (!locker.lockProcess(schedule.scheduleEntity.getProcessName(), processId)) {
              return;
            }
            processLauncher.run();
            processCompletedCount.incrementAndGet();
          } catch (Exception ex) {
            processFailedToExecuteCount.incrementAndGet();
            logContext(log.atSevere(), processName, processId)
                .withCause(ex)
                .log("Failed to execute process");
          } finally {
            schedule.getScheduleEntity().endExecution();
            scheduleService.saveProcessSchedule(schedule.getScheduleEntity());
            locker.unlockProcess(schedule.getScheduleEntity().getProcessName(), processId);
            activeProcesses.remove(processId);
            taskCompletedCount.addAndGet(processLauncher.getTaskCompletedCount());
            taskFailedCount.addAndGet(processLauncher.getTaskFailedCount());
          }
        });
  }

  private boolean validateProcess(Schedule schedule, Process process) {
    if (process == null) {
      return false;
    }

    boolean isSuccess = process.validate(Process.ValidateMode.WITHOUT_TASKS);

    String processName = schedule.getScheduleEntity().getProcessName();

    if (!processName.equals(process.getProcessName())) {
      process
          .logContext(log.atSevere())
          .log("Process name is different from launcher process name: %s", processName);
      isSuccess = false;
    }

    return isSuccess;
  }

  @Override
  protected void shutDown() throws Exception {
    logContext(log.atInfo()).log("Shutting down launcher");

    executorService.shutdown();
    try {
      executorService.awaitTermination(ServerManager.FORCE_STOP_WAIT_SECONDS - 1, TimeUnit.SECONDS);
    } catch (InterruptedException ex) {
      executorService.shutdownNow();
      throw ex;
    } finally {
      locker.unlockLauncher();

      logContext(log.atInfo()).log("Launcher has been shut down");
    }
  }

  public int getActiveProcessCount() {
    return activeProcesses.size();
  }

  public int getProcessFailedToCreateCount() {
    return processFailedToCreateCount.get();
  }

  public int getProcessFailedToExecuteCount() {
    return processFailedToExecuteCount.get();
  }

  public int getProcessCompletedCount() {
    return processCompletedCount.get();
  }

  public int getTaskFailedCount() {
    return taskFailedCount.get();
  }

  public int getTaskCompletedCount() {
    return taskCompletedCount.get();
  }

  public Long getMaxIterations() {
    return maxIterations;
  }

  public void setMaxIterations(Long maxIterations) {
    this.maxIterations = maxIterations;
  }

  private FluentLogger.Api logContext(FluentLogger.Api log) {
    return log.with(LogKey.LAUNCHER_NAME, launcherName);
  }

  private FluentLogger.Api logContext(FluentLogger.Api log, String processName, String processId) {
    return log.with(LogKey.LAUNCHER_NAME, launcherName)
        .with(LogKey.PROCESS_NAME, processName)
        .with(LogKey.PROCESS_ID, processId);
  }
}
