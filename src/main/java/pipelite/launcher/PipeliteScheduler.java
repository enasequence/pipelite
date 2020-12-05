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
import java.util.stream.Stream;

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
import pipelite.process.ProcessFactoryCache;
import pipelite.service.*;

@Flogger
public class PipeliteScheduler extends ProcessLauncherService {

  private final ScheduleService scheduleService;
  private final ProcessService processService;
  private final ProcessFactoryCache processFactoryCache;
  private final List<Schedule> schedules = Collections.synchronizedList(new ArrayList<>());
  private final Map<String, AtomicLong> maximumExecutions = new ConcurrentHashMap<>();
  private final Map<String, ProcessLauncherStats> stats = new ConcurrentHashMap<>();
  private final Duration scheduleRefreshFrequency;
  private LocalDateTime scheduleValidUntil;

  public static class Schedule {
    private final ScheduleEntity scheduleEntity;
    private LocalDateTime launchTime;

    public Schedule(ScheduleEntity scheduleEntity) {
      Assert.notNull(scheduleEntity, "Missing schedule entity");
      this.scheduleEntity = scheduleEntity;
    }

    public void newExecution() {
      launchTime = CronUtils.launchTime(scheduleEntity.getCron());
    }

    public void resumeExecution() {
      Assert.notNull(scheduleEntity.getStartTime(), "Missing launch time");
      launchTime = scheduleEntity.getStartTime();
    }

    public ScheduleEntity getScheduleEntity() {
      return scheduleEntity;
    }

    public LocalDateTime getLaunchTime() {
      return launchTime;
    }

    public String getPipelineName() {
      return scheduleEntity.getPipelineName();
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
        LauncherConfiguration.getSchedulerName(launcherConfiguration),
        processLauncherPoolSupplier);
    Assert.notNull(launcherConfiguration, "Missing launcher configuration");
    Assert.notNull(processFactoryService, "Missing process factory service");
    Assert.notNull(scheduleService, "Missing schedule service");
    Assert.notNull(processService, "Missing process service");
    this.processFactoryCache = new ProcessFactoryCache(processFactoryService);
    this.scheduleService = scheduleService;
    this.processService = processService;
    this.scheduleRefreshFrequency = launcherConfiguration.getScheduleRefreshFrequency();
  }

  @Override
  protected void run() {
    if (isScheduleProcesses()) {
      scheduleProcesses();
    }
    getExecutableSchedules()
        .forEach(schedule -> runProcess(schedule, createNewProcessEntity(schedule)));
  }

  @Override
  protected boolean shutdownIfIdle() {
    return !(maximumExecutions.isEmpty()
        || maximumExecutions.values().stream().anyMatch(r -> r.get() > 0));
  }

  public boolean isScheduleProcesses() {
    return schedules.isEmpty() || scheduleValidUntil.isBefore(LocalDateTime.now());
  }

  private void scheduleProcesses() {
    logContext(log.atInfo()).log("Scheduling processes");
    schedules.clear();
    scheduleValidUntil = LocalDateTime.now().plus(scheduleRefreshFrequency);
    for (ScheduleEntity scheduleEntity :
        scheduleService.getAllProcessSchedules(getLauncherName())) {
      scheduleProcess(scheduleEntity);
    }
  }

  private void scheduleProcess(ScheduleEntity scheduleEntity) {
    Schedule schedule = createSchedule(scheduleEntity);
    if (schedule == null) {
      return;
    }
    schedules.add(schedule);
    if (isResumeProcess(schedule)) {
      resumeProcess(schedule);
    } else {
      logContext(log.atInfo(), scheduleEntity.getPipelineName())
          .log(
              "Scheduling process execution: %s cron %s (%s)",
              schedule.getLaunchTime(), scheduleEntity.getCron(), scheduleEntity.getDescription());
      schedule.newExecution();
    }
  }

  private Schedule createSchedule(ScheduleEntity scheduleEntity) {
    // Update cron description.
    String description = CronUtils.describe(scheduleEntity.getCron());
    if (!description.equals(scheduleEntity.getDescription())) {
      scheduleEntity.setDescription(description);
      scheduleService.saveProcessSchedule(scheduleEntity);
    }
    if (!CronUtils.validate(scheduleEntity.getCron())) {
      logContext(log.atSevere(), scheduleEntity.getPipelineName())
          .log("Invalid cron expression: %s", scheduleEntity.getCron());
      return null;
    }
    return new Schedule(scheduleEntity);
  }

  private boolean isResumeProcess(Schedule schedule) {
    ScheduleEntity scheduleEntity = schedule.getScheduleEntity();
    return (scheduleEntity.getStartTime() != null
        && scheduleEntity.getEndTime() == null
        && scheduleEntity.getProcessId() != null);
  }

  private void resumeProcess(Schedule schedule) {
    logContext(log.atInfo(), schedule.getPipelineName()).log("Resuming process execution");
    schedule.resumeExecution();
    runProcess(schedule, createResumeProcessEntity(schedule));
  }

  private ProcessEntity createResumeProcessEntity(Schedule schedule) {
    return processService
        .getSavedProcess(schedule.getPipelineName(), schedule.getScheduleEntity().getProcessId())
        .get();
  }

  private ProcessEntity createNewProcessEntity(Schedule schedule) {
    String lastProcessId = schedule.getScheduleEntity().getProcessId();
    String nextProcessId = nextProcessId(lastProcessId);

    String pipelineName = schedule.getPipelineName();
    Optional<ProcessEntity> processEntity =
        processService.getSavedProcess(pipelineName, nextProcessId);

    if (processEntity.isPresent()) {
      lastProcessId = processService.getMaxProcessId(pipelineName);
      if (lastProcessId.matches("\\d+")) {
        nextProcessId = nextProcessId(lastProcessId);
      } else {
        logContext(log.atSevere(), pipelineName)
            .log("Could not create next process id. Last process id is %s", lastProcessId);
        return null;
      }
    }
    return processService.createExecution(
        pipelineName, nextProcessId, ProcessEntity.DEFAULT_PRIORITY);
  }

  public static String nextProcessId(String lastProcessId) {
    if (lastProcessId == null) {
      return "1";
    }
    try {
      return String.valueOf(Long.valueOf(lastProcessId) + 1);
    } catch (Exception ex) {
      throw new RuntimeException("Invalid process id " + lastProcessId);
    }
  }

  private void runProcess(Schedule schedule, ProcessEntity processEntity) {
    ScheduleEntity scheduleEntity = schedule.getScheduleEntity();
    String pipelineName = scheduleEntity.getPipelineName();
    String processId = processEntity.getProcessId();
    logContext(log.atInfo(), pipelineName, processId).log("Executing scheduled process");

    scheduleEntity.startExecution(processId);
    scheduleService.saveProcessSchedule(scheduleEntity);

    Process process =
        ProcessFactory.create(processEntity, processFactoryCache.getProcessFactory(pipelineName));
    if (process == null) {
      stats.get(pipelineName).addProcessCreationFailedCount(1);
      logContext(log.atSevere(), pipelineName, processId).log("Failed to create scheduled process");
    } else {
      maximumExecutions.get(pipelineName).decrementAndGet();
      runProcess(
          pipelineName,
          process,
          (p, r) -> {
            scheduleEntity.endExecution();
            scheduleService.saveProcessSchedule(scheduleEntity);
            schedule.newExecution();
            stats(pipelineName).add(p, r);
          });
    }
  }

  public Collection<Schedule> getSchedules() {
    return this.schedules;
  }

  public Stream<Schedule> getPendingSchedules() {
    return this.schedules.stream()
        .filter(schedule -> !isPipelineActive(schedule.scheduleEntity.getPipelineName()));
  }

  public Stream<Schedule> getExecutableSchedules() {
    return getPendingSchedules()
        .filter(schedule -> !schedule.getLaunchTime().isAfter(LocalDateTime.now()))
        .filter(
            schedule ->
                maximumExecutions.get(schedule.getPipelineName()) == null
                    || maximumExecutions.get(schedule.getPipelineName()).get() > 0);
  }

  public void setMaximumExecutions(String pipelineName, long maximumExecutions) {
    this.maximumExecutions.putIfAbsent(pipelineName, new AtomicLong());
    this.maximumExecutions.get(pipelineName).set(maximumExecutions);
  }

  private ProcessLauncherStats stats(String pipelineName) {
    stats.putIfAbsent(pipelineName, new ProcessLauncherStats());
    return stats.get(pipelineName);
  }

  public ProcessLauncherStats getStats(String pipelineName) {
    return stats.get(pipelineName);
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
