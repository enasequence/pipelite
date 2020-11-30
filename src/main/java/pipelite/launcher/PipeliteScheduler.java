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

import com.google.common.flogger.FluentLogger;
import com.google.common.util.concurrent.AbstractScheduledService;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import lombok.Data;
import lombok.extern.flogger.Flogger;
import org.springframework.util.Assert;
import pipelite.configuration.LauncherConfiguration;
import pipelite.configuration.StageConfiguration;
import pipelite.cron.CronUtils;
import pipelite.entity.LauncherLockEntity;
import pipelite.entity.ProcessEntity;
import pipelite.entity.ScheduleEntity;
import pipelite.log.LogKey;
import pipelite.process.Process;
import pipelite.process.ProcessFactory;
import pipelite.service.*;

@Flogger
public class PipeliteScheduler extends AbstractScheduledService {

  private static final int DEFAULT_MAX_PROCESS_ID_RETRIES = 100;
  private final LauncherConfiguration launcherConfiguration;
  private final StageConfiguration stageConfiguration;
  private final ProcessFactoryService processFactoryService;
  private final ScheduleService scheduleService;
  private final ProcessService processService;
  private final StageService stageService;
  private final LockService lockService;
  private final MailService mailService;
  private final String schedulerName;
  private final Duration processLaunchFrequency;
  private final Duration processRefreshFrequency;
  private final Map<String, ProcessFactory> processFactoryCache = new ConcurrentHashMap<>();
  private final Map<String, ProcessLauncherStats> stats = new ConcurrentHashMap<>();
  private final Map<String, AtomicLong> remainingExecutions = new ConcurrentHashMap<>();
  private final List<Schedule> schedules = Collections.synchronizedList(new ArrayList<>());
  private LocalDateTime schedulesValidUntil;
  private boolean shutdownAfterTriggered = false;

  private LauncherLockEntity lock;
  private ProcessLauncherPool pool;

  @Data
  public static class Schedule {
    private ScheduleEntity scheduleEntity;
    private ProcessEntity processEntity;
    private Process process;
    private LocalDateTime launchTime;
  }

  public PipeliteScheduler(
      LauncherConfiguration launcherConfiguration,
      StageConfiguration stageConfiguration,
      ProcessFactoryService processFactoryService,
      ScheduleService scheduleService,
      ProcessService processService,
      StageService stageService,
      LockService lockService,
      MailService mailService) {
    Assert.notNull(launcherConfiguration, "Missing launcher configuration");
    Assert.notNull(stageConfiguration, "Missing stage configuration");
    Assert.notNull(processFactoryService, "Missing process factory service");
    Assert.notNull(processService, "Missing process service");
    Assert.notNull(scheduleService, "Missing schedule service");
    Assert.notNull(stageService, "Missing stage service");
    Assert.notNull(lockService, "Missing lock service");
    Assert.notNull(mailService, "Missing mail service");
    this.launcherConfiguration = launcherConfiguration;
    this.stageConfiguration = stageConfiguration;
    this.processFactoryService = processFactoryService;
    this.scheduleService = scheduleService;
    this.processService = processService;
    this.stageService = stageService;
    this.lockService = lockService;
    this.mailService = mailService;
    this.schedulerName = LauncherConfiguration.getSchedulerName(launcherConfiguration);
    if (launcherConfiguration.getProcessLaunchFrequency() != null) {
      this.processLaunchFrequency = launcherConfiguration.getProcessLaunchFrequency();
    } else {
      this.processLaunchFrequency = LauncherConfiguration.DEFAULT_PROCESS_LAUNCH_FREQUENCY;
    }
    if (launcherConfiguration.getProcessRefreshFrequency() != null) {
      this.processRefreshFrequency = launcherConfiguration.getProcessRefreshFrequency();
    } else {
      this.processRefreshFrequency = LauncherConfiguration.DEFAULT_PROCESS_REFRESH_FREQUENCY;
    }
  }

  @Override
  protected void startUp() {
    logContext(log.atInfo()).log("Starting up scheduler");
    List<LauncherLockEntity> locks = lockService.getLauncherLocksByLauncherName(schedulerName);
    if (!locks.isEmpty()) {
      throw new RuntimeException(
          "Could not start scheduler " + schedulerName + ": scheduler is already locked");
    }
    lock = lockService.lockLauncher(schedulerName);
    locks = lockService.getLauncherLocksByLauncherName(schedulerName);
    if (locks.size() != 1 || !locks.get(0).getLauncherId().equals(lock.getLauncherId())) {
      lockService.unlockLauncher(lock);
      throw new RuntimeException(
          "Could not start scheduler " + schedulerName + ": scheduler is already locked");
    }
    this.pool =
        new ProcessLauncherPool(
            lockService,
            (pipelineName, process) ->
                new ProcessLauncher(
                    launcherConfiguration,
                    stageConfiguration,
                    processService,
                    stageService,
                    mailService,
                    pipelineName,
                    process),
            lock);
  }

  @Override
  public String serviceName() {
    return schedulerName;
  }

  @Override
  protected Scheduler scheduler() {
    return Scheduler.newFixedDelaySchedule(Duration.ZERO, processLaunchFrequency);
  }

  @Override
  protected void runOneIteration() {
    if (shutdownAfterTriggered || !isRunning()) {
      return;
    }
    logContext(log.atInfo()).log("Running scheduler");
    // Renew scheduler lock to avoid lock expiry.
    if (!lockService.relockLauncher(lock)) {
      throw new RuntimeException(
          "Could not continue running scheduler " + schedulerName + ": could not renew lock");
    }

    if (schedules.isEmpty() || schedulesValidUntil.isBefore(LocalDateTime.now())) {
      scheduleProcesses();
    }
    for (Schedule schedule : schedules) {
      String pipelineName = schedule.getScheduleEntity().getPipelineName();
      String cronExpression = schedule.getScheduleEntity().getSchedule();
      LocalDateTime launchTime = schedule.getLaunchTime();
      if (!pool.hasActivePipeline(pipelineName) && launchTime.isBefore(LocalDateTime.now())) {
        if (remainingExecutions.get(pipelineName) != null
            && remainingExecutions.get(pipelineName).decrementAndGet() < 0) {
          continue;
        }
        // Set next launch time.
        schedule.setLaunchTime(CronUtils.launchTime(cronExpression));
        logContext(log.atInfo(), pipelineName)
            .log(
                "Launching %s pipeline with cron expression %s (%s). Next launch time is: %s",
                pipelineName,
                cronExpression,
                schedule.getScheduleEntity().getDescription(),
                launchTime);
        if (createProcess(schedule)) {
          launchProcess(schedule);
        }
      }
    }
    shutdownIfNoRemainingExecutions();
  }

  private void shutdownIfNoRemainingExecutions() {
    if (remainingExecutions.isEmpty()) {
      return;
    }
    for (AtomicLong remaining : remainingExecutions.values()) {
      if (remaining.get() > 0) {
        return;
      }
    }
    logContext(log.atInfo())
        .log("Stopping pipelite scheduler " + schedulerName + " after maximum executions");
    shutdownAfterTriggered = true;
    stopAsync();
  }

  private void scheduleProcesses() {
    logContext(log.atInfo()).log("Scheduling processes");
    schedules.clear();
    List<ScheduleEntity> scheduleEntities = scheduleService.getAllProcessSchedules(schedulerName);
    logContext(log.atInfo()).log("Found %s schedules", scheduleEntities.size());
    for (ScheduleEntity scheduleEntity : scheduleEntities) {
      String scheduleDescription = "invalid cron expression";
      if (CronUtils.validate(scheduleEntity.getSchedule())) {
        Schedule schedule = new Schedule();
        schedule.setScheduleEntity(scheduleEntity);
        schedules.add(schedule);

        // TODO: test resume process execution
        boolean isResumeProcessExecution = resumeProcessExecution(schedule);
        if (isResumeProcessExecution) {
          logContext(log.atInfo(), scheduleEntity.getPipelineName())
              .log("Attempting to resume %s pipeline execution", scheduleEntity.getPipelineName());
          launchProcess(schedule);
        } else {
          // Update next launch time.
          schedule.setLaunchTime(CronUtils.launchTime(scheduleEntity.getSchedule()));
          scheduleDescription = CronUtils.describe(scheduleEntity.getSchedule());
          logContext(log.atInfo(), scheduleEntity.getPipelineName())
              .log(
                  "Scheduling %s pipeline with cron expression %s (%s). Next launch time is: %s",
                  scheduleEntity.getPipelineName(),
                  scheduleEntity.getSchedule(),
                  scheduleDescription,
                  schedule.getLaunchTime());
        }
      } else {
        logContext(log.atSevere(), scheduleEntity.getPipelineName())
            .log(
                "Ignoring %s pipeline with invalid cron expression",
                scheduleEntity.getPipelineName());
      }
      if (!scheduleDescription.equals(scheduleEntity.getDescription())) {
        scheduleEntity.setDescription(scheduleDescription);
        scheduleService.saveProcessSchedule(scheduleEntity);
      }
    }
    schedulesValidUntil = LocalDateTime.now().plus(processRefreshFrequency);
  }

  private boolean resumeProcessExecution(Schedule schedule) {
    ScheduleEntity scheduleEntity = schedule.getScheduleEntity();
    String pipelineName = scheduleEntity.getPipelineName();
    String processId = scheduleEntity.getProcessId();
    if (scheduleEntity.getStartTime() != null
        && scheduleEntity.getEndTime() == null
        && processId != null) {
      Process process = createProcess(pipelineName, processId);
      Optional<ProcessEntity> processEntity =
          processService.getSavedProcess(pipelineName, processId);
      if (process != null && processEntity.isPresent()) {
        schedule.setProcess(process);
        schedule.setProcessEntity(processEntity.get());
        return true;
      } else {
        scheduleEntity.resetExecution();
        scheduleService.saveProcessSchedule(scheduleEntity);
        return false;
      }
    }
    return false;
  }

  public static String getNextProcessId(String processId) {
    if (processId == null) {
      return "1";
    }
    try {
      return String.valueOf(Integer.valueOf(processId) + 1);
    } catch (Exception ex) {
      throw new RuntimeException("Invalid process id " + processId);
    }
  }

  private ProcessFactory getCachedProcessFactory(String pipelineName) {
    if (processFactoryCache.containsKey(pipelineName)) {
      return processFactoryCache.get(pipelineName);
    }
    ProcessFactory processFactory = processFactoryService.create(pipelineName);
    processFactoryCache.put(pipelineName, processFactory);
    return processFactory;
  }

  private boolean createProcess(Schedule schedule) {
    String pipelineName = schedule.getScheduleEntity().getPipelineName();
    String processId = getNextProcessId(schedule.getScheduleEntity().getProcessId());
    int remainingProcessIdRetries = DEFAULT_MAX_PROCESS_ID_RETRIES;
    while (true) {
      Optional<ProcessEntity> savedProcessEntity =
          processService.getSavedProcess(pipelineName, processId);
      if (savedProcessEntity.isPresent()) {
        processId = getNextProcessId(savedProcessEntity.get().getProcessId());
      } else {
        break;
      }
      if (--remainingProcessIdRetries <= 0) {
        throw new RuntimeException("Could not assign a new process id");
      }
    }
    Process process = createProcess(pipelineName, processId);
    if (process == null) {
      return false;
    }
    ProcessEntity processEntity =
        processService.createExecution(pipelineName, processId, ProcessEntity.DEFAULT_PRIORITY);
    schedule.setProcess(process);
    schedule.setProcessEntity(processEntity);
    schedule.getScheduleEntity().startExecution(processId);
    scheduleService.saveProcessSchedule(schedule.getScheduleEntity());
    return true;
  }

  private Process createProcess(String pipelineName, String processId) {
    logContext(log.atInfo(), pipelineName, processId).log("Creating new process");
    ProcessFactory processFactory = getCachedProcessFactory(pipelineName);
    Process process = processFactory.create(processId);
    if (process == null) {
      logContext(log.atSevere(), processId).log("Failed to create process: %s", processId);
      stats(pipelineName).processCreationFailedCount.incrementAndGet();
      return null;
    }
    return process;
  }

  private void launchProcess(Schedule schedule) {
    Process process = schedule.getProcess();
    ProcessEntity processEntity = schedule.getProcessEntity();
    String pipelineName = processEntity.getPipelineName();
    String processId = processEntity.getProcessId();
    logContext(log.atInfo(), pipelineName, processId).log("Launching process");

    process.setProcessEntity(processEntity);
    pool.run(
        pipelineName,
        process,
        (p, r) -> {
          schedule.getScheduleEntity().endExecution();
          scheduleService.saveProcessSchedule(schedule.getScheduleEntity());
          stats(pipelineName).add(p, r);
        });
  }

  @Override
  protected void shutDown() throws InterruptedException {
    logContext(log.atInfo()).log("Shutting down scheduler");
    pool.shutDown();
    logContext(log.atInfo()).log("Scheduler has been shut down");
  }

  public String getSchedulerName() {
    return schedulerName;
  }

  public List<Schedule> getSchedules() {
    return this.schedules;
  }

  public void removeLocks() {
    List<LauncherLockEntity> launcherLocks =
        lockService.getLauncherLocksByLauncherName(schedulerName);
    for (LauncherLockEntity launcherLock : launcherLocks) {
      lockService.unlockProcesses(launcherLock);
      lockService.unlockLauncher(launcherLock);
    }
  }

  private ProcessLauncherStats stats(String pipelineName) {
    stats.putIfAbsent(pipelineName, new ProcessLauncherStats());
    return stats.get(pipelineName);
  }

  public List<ProcessLauncher> getProcessLaunchers() {
    return pool.get();
  }

  public ProcessLauncherStats getStats(String pipelineName) {
    return stats.get(pipelineName);
  }

  public void setMaximumExecutions(String pipelineName, long maximumExecutions) {
    this.remainingExecutions.putIfAbsent(pipelineName, new AtomicLong());
    this.remainingExecutions.get(pipelineName).set(maximumExecutions);
  }

  private FluentLogger.Api logContext(FluentLogger.Api log) {
    return log.with(LogKey.LAUNCHER_NAME, schedulerName);
  }

  private FluentLogger.Api logContext(FluentLogger.Api log, String pipelineName) {
    return log.with(LogKey.LAUNCHER_NAME, schedulerName).with(LogKey.PIPELINE_NAME, pipelineName);
  }

  private FluentLogger.Api logContext(FluentLogger.Api log, String pipelineName, String processId) {
    return log.with(LogKey.LAUNCHER_NAME, schedulerName)
        .with(LogKey.PIPELINE_NAME, pipelineName)
        .with(LogKey.PROCESS_ID, processId);
  }
}
