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
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import lombok.extern.flogger.Flogger;
import org.springframework.util.Assert;
import pipelite.configuration.LauncherConfiguration;
import pipelite.cron.CronUtils;
import pipelite.entity.ProcessEntity;
import pipelite.entity.ScheduleEntity;
import pipelite.lock.PipeliteLocker;
import pipelite.log.LogKey;
import pipelite.process.Process;
import pipelite.process.ProcessFactory;
import pipelite.service.*;

@Flogger
public class PipeliteScheduler extends ProcessLauncherService {

  private static final int MAX_PROCESS_ID_RETRIES = 100;
  private final ProcessFactoryService processFactoryService;
  private final ScheduleService scheduleService;
  private final ProcessService processService;
  private final Duration scheduleRefreshFrequency;
  private final Map<String, ProcessFactory> processFactoryCache = new ConcurrentHashMap<>();
  private final Map<String, ProcessLauncherStats> stats = new ConcurrentHashMap<>();
  private final Map<String, AtomicLong> maximumExecutions = new ConcurrentHashMap<>();
  private final List<Schedule> schedules = Collections.synchronizedList(new ArrayList<>());
  private LocalDateTime schedulesValidUntil;

  public static class Schedule {
    private final ScheduleEntity scheduleEntity;
    private LocalDateTime launchTime;

    public Schedule(ScheduleEntity scheduleEntity) {
      Assert.notNull(scheduleEntity, "Missing schedule entity");
      this.scheduleEntity = scheduleEntity;
    }

    public void executeNow(LocalDateTime launchTime) {
      Assert.notNull(launchTime, "Missing launch time");
      this.launchTime = launchTime;
    }

    public void executeLater() {
      launchTime = CronUtils.launchTime(scheduleEntity.getCron());
    }

    public ScheduleEntity getScheduleEntity() {
      return scheduleEntity;
    }

    public LocalDateTime getLaunchTime() {
      return launchTime;
    }
  }

  public PipeliteScheduler(
      LauncherConfiguration launcherConfiguration,
      PipeliteLocker pipeliteLocker,
      ProcessFactoryService processFactoryService,
      ScheduleService scheduleService,
      ProcessService processService,
      Supplier<ProcessLauncherPool> processLauncherPoolSupplier) {
    super(
        launcherConfiguration,
        pipeliteLocker,
        launcherName(launcherConfiguration),
        processLauncherPoolSupplier);
    Assert.notNull(launcherConfiguration, "Missing launcher configuration");
    Assert.notNull(processFactoryService, "Missing process factory service");
    Assert.notNull(processService, "Missing process service");
    Assert.notNull(scheduleService, "Missing schedule service");
    this.processFactoryService = processFactoryService;
    this.scheduleService = scheduleService;
    this.processService = processService;
    this.scheduleRefreshFrequency = launcherConfiguration.getScheduleRefreshFrequency();
  }

  private static String launcherName(LauncherConfiguration launcherConfiguration) {
    return LauncherConfiguration.getSchedulerName(launcherConfiguration);
  }

  @Override
  protected void run() {
    if (schedules.isEmpty() || schedulesValidUntil.isBefore(LocalDateTime.now())) {
      scheduleProcesses();
    }
    for (Schedule schedule : schedules) {
      ScheduleEntity scheduleEntity = schedule.getScheduleEntity();
      String pipelineName = scheduleEntity.getPipelineName();
      if (!isPipelineActive(pipelineName)
          && !schedule.getLaunchTime().isAfter(LocalDateTime.now())) {
        if (maximumExecutions.get(pipelineName) != null
            && maximumExecutions.get(pipelineName).decrementAndGet() < 0) {
          continue;
        }
        runProcess(schedule, createProcessEntity(schedule));
      }
    }
  }

  @Override
  protected boolean shutdownIfIdle() {
    return !(maximumExecutions.isEmpty()
        || maximumExecutions.values().stream().anyMatch(r -> r.get() > 0));
  }

  private void scheduleProcesses() {
    logContext(log.atInfo()).log("Scheduling processes");
    schedules.clear();
    schedulesValidUntil = LocalDateTime.now().plus(scheduleRefreshFrequency);
    for (ScheduleEntity scheduleEntity :
        scheduleService.getAllProcessSchedules(getLauncherName())) {
      Schedule schedule = createSchedule(scheduleEntity);
      if (schedule == null) {
        continue;
      }
      schedules.add(schedule);
      if (!resumeProcess(schedule)) {
        logContext(log.atInfo(), scheduleEntity.getPipelineName())
            .log(
                "Scheduling process execution: %s cron %s (%s)",
                schedule.getLaunchTime(),
                scheduleEntity.getCron(),
                scheduleEntity.getDescription());
        schedule.executeLater();
      }
    }
  }

  private boolean resumeProcess(Schedule schedule) {
    ProcessEntity processEntity = createResumedProcessEntity(schedule);
    if (processEntity != null) {
      logContext(log.atInfo(), schedule.getScheduleEntity().getPipelineName())
          .log("Resuming process execution");
      schedule.executeNow(LocalDateTime.now());
      runProcess(schedule, processEntity);
      return true;
    }
    return false;
  }

  private Schedule createSchedule(ScheduleEntity scheduleEntity) {
    updateCronDescription(scheduleEntity);
    if (!CronUtils.validate(scheduleEntity.getCron())) {
      logContext(log.atSevere(), scheduleEntity.getPipelineName())
          .log("Invalid cron expression: %s", scheduleEntity.getCron());
      return null;
    }
    return new Schedule(scheduleEntity);
  }

  private ProcessEntity createResumedProcessEntity(Schedule schedule) {
    ScheduleEntity scheduleEntity = schedule.getScheduleEntity();
    String processId = scheduleEntity.getProcessId();
    if (scheduleEntity.getStartTime() != null
        && scheduleEntity.getEndTime() == null
        && processId != null) {
      String pipelineName = scheduleEntity.getPipelineName();
      Optional<ProcessEntity> processEntity =
          processService.getSavedProcess(pipelineName, processId);
      if (processEntity.isPresent()) {
        return processEntity.get();
      }
    }
    return null;
  }

  public static String getNextProcessId(String processId) {
    if (processId == null) {
      return "1";
    }
    try {
      return String.valueOf(Long.valueOf(processId) + 1);
    } catch (Exception ex) {
      throw new RuntimeException("Invalid process id " + processId);
    }
  }

  private ProcessEntity createProcessEntity(Schedule schedule) {
    String pipelineName = schedule.getScheduleEntity().getPipelineName();
    String processId = null;
    for (int retries = MAX_PROCESS_ID_RETRIES; retries > 0; --retries) {
      processId = getNextProcessId(schedule.getScheduleEntity().getProcessId());
      Optional<ProcessEntity> savedProcessEntity =
          processService.getSavedProcess(pipelineName, processId);
      if (!savedProcessEntity.isPresent()) {
        break;
      }
    }
    if (processId == null) {
      logContext(log.atSevere(), pipelineName).log("Could not create process id");
      return null;
    }
    return processService.createExecution(pipelineName, processId, ProcessEntity.DEFAULT_PRIORITY);
  }

  private void updateCronDescription(ScheduleEntity scheduleEntity) {
    String description = CronUtils.describe(scheduleEntity.getCron());
    if (!description.equals(scheduleEntity.getDescription())) {
      scheduleEntity.setDescription(description);
      scheduleService.saveProcessSchedule(scheduleEntity);
    }
  }

  private void runProcess(Schedule schedule, ProcessEntity processEntity) {
    ScheduleEntity scheduleEntity = schedule.getScheduleEntity();
    String pipelineName = scheduleEntity.getPipelineName();
    String processId = processEntity.getProcessId();
    logContext(log.atInfo(), pipelineName, processId).log("Executing process");
    scheduleEntity.startExecution(processId);
    scheduleService.saveProcessSchedule(scheduleEntity);
    Process process =
        ProcessFactory.create(
            processEntity, processFactoryCache(pipelineName), stats(pipelineName));
    if (process == null) {
      logContext(log.atSevere(), pipelineName, processId).log("Failed to create process");
      return;
    }
    run(
        pipelineName,
        process,
        (p, r) -> {
          schedule.executeLater();
          scheduleEntity.endExecution();
          scheduleService.saveProcessSchedule(scheduleEntity);
          stats(pipelineName).add(p, r);
        });
  }

  private ProcessFactory processFactoryCache(String pipelineName) {
    if (processFactoryCache.containsKey(pipelineName)) {
      return processFactoryCache.get(pipelineName);
    }
    ProcessFactory processFactory = processFactoryService.create(pipelineName);
    processFactoryCache.put(pipelineName, processFactory);
    return processFactory;
  }

  public List<Schedule> getSchedules() {
    return this.schedules;
  }

  private ProcessLauncherStats stats(String pipelineName) {
    stats.putIfAbsent(pipelineName, new ProcessLauncherStats());
    return stats.get(pipelineName);
  }

  public ProcessLauncherStats getStats(String pipelineName) {
    return stats.get(pipelineName);
  }

  public void setMaximumExecutions(String pipelineName, long maximumExecutions) {
    this.maximumExecutions.putIfAbsent(pipelineName, new AtomicLong());
    this.maximumExecutions.get(pipelineName).set(maximumExecutions);
  }

  private FluentLogger.Api logContext(FluentLogger.Api log) {
    return log.with(LogKey.LAUNCHER_NAME, getLauncherName());
  }

  private FluentLogger.Api logContext(FluentLogger.Api log, String pipelineName) {
    return log.with(LogKey.LAUNCHER_NAME, getLauncherName())
        .with(LogKey.PIPELINE_NAME, pipelineName);
  }

  private FluentLogger.Api logContext(FluentLogger.Api log, String pipelineName, String processId) {
    return log.with(LogKey.LAUNCHER_NAME, getLauncherName())
        .with(LogKey.PIPELINE_NAME, pipelineName)
        .with(LogKey.PROCESS_ID, processId);
  }
}
