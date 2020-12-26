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
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Duration;
import java.time.ZonedDateTime;
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
import pipelite.exception.PipeliteException;
import pipelite.launcher.process.runner.ProcessRunnerPool;
import pipelite.launcher.process.runner.ProcessRunnerPoolService;
import pipelite.launcher.process.runner.ProcessRunnerStats;
import pipelite.lock.PipeliteLocker;
import pipelite.log.LogKey;
import pipelite.process.Process;
import pipelite.process.ProcessFactory;
import pipelite.process.ProcessFactoryCache;
import pipelite.schedule.Schedule;
import pipelite.service.*;

/**
 * Executes non-parallel processes using cron schedules. New process instances are created using
 * {@link pipelite.process.ProcessFactory}. New process ids are inserted into the PIPELITE_PROCESS
 * table by the {@link pipelite.launcher.PipeliteScheduler}.
 */
@Flogger
public class PipeliteScheduler extends ProcessRunnerPoolService {

  private final ScheduleService scheduleService;
  private final ProcessService processService;
  private final ProcessFactoryCache processFactoryCache;
  private final List<Schedule> schedules = Collections.synchronizedList(new ArrayList<>());
  private final Map<String, AtomicLong> maximumExecutions = new ConcurrentHashMap<>();
  private final Map<String, ProcessRunnerStats> stats = new ConcurrentHashMap<>();
  private final Duration scheduleRefreshFrequency;
  private ZonedDateTime scheduleValidUntil = ZonedDateTime.now();
  private MeterRegistry meterRegistry;

  public PipeliteScheduler(
      LauncherConfiguration launcherConfiguration,
      PipeliteLocker pipeliteLocker,
      ProcessFactoryService processFactoryService,
      ScheduleService scheduleService,
      ProcessService processService,
      Supplier<ProcessRunnerPool> processRunnerPoolSupplier,
      MeterRegistry meterRegistry) {
    super(
        launcherConfiguration,
        pipeliteLocker,
        LauncherConfiguration.getSchedulerName(launcherConfiguration),
        processRunnerPoolSupplier);
    Assert.notNull(launcherConfiguration, "Missing launcher configuration");
    Assert.notNull(processFactoryService, "Missing process factory service");
    Assert.notNull(scheduleService, "Missing schedule service");
    Assert.notNull(processService, "Missing process service");
    this.processFactoryCache = new ProcessFactoryCache(processFactoryService);
    this.scheduleService = scheduleService;
    this.processService = processService;
    this.scheduleRefreshFrequency = launcherConfiguration.getScheduleRefreshFrequency();
    this.meterRegistry = meterRegistry;
  }

  @Override
  protected void startUp() {
    super.startUp();
    refreshSchedules();
    resumeSchedules();
  }

  @Override
  protected void run() {
    refreshSchedules();
    executeSchedules();
    purgeStats();
  }

  protected void executeSchedules() {
    getExecutableSchedules()
        .forEach(
            s -> {
              try {
                executeSchedule(s, createProcessEntity(s));
              } catch (Exception ex) {
                logContext(log.atSevere(), s.getPipelineName()).log("Could not execute schedule");
              }
            });
  }

  @Override
  protected boolean shutdownIfIdle() {
    return !(maximumExecutions.isEmpty()
        || maximumExecutions.values().stream().anyMatch(r -> r.get() > 0));
  }

  /**
   * Returns true the if the schedules should be refreshed. The refresh frequency is defined by
   * scheduleRefreshFrequency.
   *
   * @return true the if the schedules should be refreshed
   */
  public boolean isRefreshSchedules() {
    return schedules.isEmpty() || !scheduleValidUntil.isAfter(ZonedDateTime.now());
  }

  /** Refreshes the schedules. The refresh frequency is defined by scheduleRefreshFrequency. */
  protected void refreshSchedules() {
    if (!isRefreshSchedules()) {
      return;
    }
    logContext(log.atInfo()).log("Refreshing schedules");
    scheduleValidUntil = ZonedDateTime.now().plus(scheduleRefreshFrequency);

    Collection<ScheduleEntity> scheduleEntities =
        scheduleService.getActiveSchedules(getLauncherName());

    for (ScheduleEntity scheduleEntity : scheduleEntities) {
      Optional<Schedule> schedule = findSchedule(scheduleEntity);
      if (schedule.isPresent()) {
        // Refresh existing schedule.
        refreshSchedule(schedule.get(), scheduleEntity);
      } else {
        // Create new schedule.
        createSchedule(scheduleEntity);
      }
    }
    for (Schedule schedule : schedules) {
      Optional<ScheduleEntity> scheduleEntity = findScheduleEntity(scheduleEntities, schedule);
      if (!scheduleEntity.isPresent()) {
        // Remove deleted schedule.
        schedules.remove(schedule);
      }
    }
  }

  private Optional<ScheduleEntity> findScheduleEntity(
      Collection<ScheduleEntity> scheduleEntities, Schedule schedule) {
    return scheduleEntities.stream()
        .filter(s -> s.getPipelineName().equals(schedule.getPipelineName()))
        .findFirst();
  }

  private Optional<Schedule> findSchedule(ScheduleEntity scheduleEntity) {
    return schedules.stream()
        .filter(s -> s.getPipelineName().equals(scheduleEntity.getPipelineName()))
        .findFirst();
  }
  /**
   * Creates a new schedule and sets the launch time.
   *
   * @param scheduleEntity the schedule entity
   */
  private void createSchedule(ScheduleEntity scheduleEntity) {
    Schedule schedule = new Schedule(scheduleEntity.getPipelineName());
    updateCron(scheduleEntity);
    schedule.setCron(scheduleEntity.getCron());
    schedule.setLaunchTime();
    schedules.add(schedule);
  }

  /**
   * Refreshes the schedule.
   *
   * @param scheduleEntity the schedule entity
   */
  private void refreshSchedule(Schedule schedule, ScheduleEntity scheduleEntity) {
    updateCron(scheduleEntity);
    schedule.setCron(scheduleEntity.getCron());
  }

  /**
   * Updates the cron description if it has changed.
   *
   * @param scheduleEntity the schedule entity
   */
  private void updateCron(ScheduleEntity scheduleEntity) {
    String description = CronUtils.describe(scheduleEntity.getCron());
    if (!description.equals(scheduleEntity.getDescription())) {
      scheduleEntity.setDescription(description);
      scheduleService.saveSchedule(scheduleEntity);
    }
  }

  /** Resumes process executions. */
  protected void resumeSchedules() {
    getSchedules()
        .forEach(
            s -> {
              try {
                Optional<ScheduleEntity> opt = scheduleService.geSavedSchedule(s.getPipelineName());
                ScheduleEntity scheduleEntity = opt.get();
                resumeSchedule(s, scheduleEntity);
              } catch (Exception ex) {
                logContext(log.atSevere(), s.getPipelineName()).log("Could not resume process");
              }
            });
  }

  protected void resumeSchedule(Schedule schedule, ScheduleEntity scheduleEntity) {
    if (!scheduleEntity.isResumeProcess()) {
      return;
    }
    logContext(log.atInfo(), schedule.getPipelineName()).log("Resuming process execution");
    Optional<ProcessEntity> processEntity = getSavedProcessEntity(scheduleEntity);
    if (!processEntity.isPresent()) {
      logContext(log.atSevere(), scheduleEntity.getPipelineName(), scheduleEntity.getProcessId())
          .log("Could not resume process execution because process does not exist");
    } else {
      executeSchedule(schedule, processEntity.get());
    }
  }

  private Optional<ProcessEntity> getSavedProcessEntity(ScheduleEntity scheduleEntity) {
    return processService.getSavedProcess(
        scheduleEntity.getPipelineName(), scheduleEntity.getProcessId());
  }

  private ProcessEntity createProcessEntity(Schedule schedule) {
    String pipelineName = schedule.getPipelineName();
    ScheduleEntity scheduleEntity = scheduleService.geSavedSchedule(pipelineName).get();

    String lastProcessId = scheduleEntity.getProcessId();
    String nextProcessId = nextProcessId(lastProcessId);

    Optional<ProcessEntity> processEntity =
        processService.getSavedProcess(pipelineName, nextProcessId);
    if (processEntity.isPresent()) {
      throw new PipeliteException(
          "Scheduled new process already exists: " + pipelineName + " " + nextProcessId);
    }
    return processService.createExecution(
        pipelineName, nextProcessId, ProcessEntity.DEFAULT_PRIORITY);
  }

  /**
   * Returns the next process id.
   *
   * @param lastProcessId the last process id
   * @return the next process id
   * @throws RuntimeException if a new process id could not be created
   */
  public static String nextProcessId(String lastProcessId) {
    if (lastProcessId == null) {
      return "1";
    }
    try {
      return String.valueOf(Long.valueOf(lastProcessId) + 1);
    } catch (Exception ex) {
      throw new PipeliteException("Invalid process id " + lastProcessId);
    }
  }

  protected void executeSchedule(Schedule schedule, ProcessEntity processEntity) {
    String pipelineName = processEntity.getPipelineName();
    String processId = processEntity.getProcessId();
    logContext(log.atInfo(), pipelineName, processId).log("Executing scheduled process");

    Process process =
        ProcessFactory.create(processEntity, processFactoryCache.getProcessFactory(pipelineName));
    if (process == null) {
      getStats(pipelineName).addProcessCreationFailed(1);
      logContext(log.atSevere(), pipelineName, processId).log("Failed to create scheduled process");
      return;
    }
    maximumExecutions.get(pipelineName).decrementAndGet();

    scheduleService.startExecution(pipelineName, processId);
    schedule.removeLaunchTime();

    runProcess(
        pipelineName,
        process,
        (p, r) -> {
          scheduleService.endExecution(pipelineName, processEntity);
          schedule.setLaunchTime();
          getStats(pipelineName).addProcessRunnerResult(p.getProcessEntity().getState(), r);
        });
  }

  /**
   * Returns all schedules.
   *
   * @return all schedules.
   */
  public List<Schedule> getSchedules() {
    return this.schedules;
  }

  /**
   * Returns all stages that are not running and can be executed.
   *
   * @return all stages that are not running and can be executed.
   */
  public Stream<Schedule> getExecutableSchedules() {
    return getSchedules().stream()
        // Must have launch time.
        .filter(s -> s.getLaunchTime() != null)
        // Must not be running.
        .filter(s -> !isPipelineActive(s.getPipelineName()))
        // Must not have exceeded maximum executions.
        .filter(
            schedule ->
                maximumExecutions.get(schedule.getPipelineName()) == null
                    || maximumExecutions.get(schedule.getPipelineName()).get() > 0)
        .filter(schedule -> !schedule.getLaunchTime().isAfter(ZonedDateTime.now()));
  }

  public void setMaximumExecutions(String pipelineName, long maximumExecutions) {
    this.maximumExecutions.putIfAbsent(pipelineName, new AtomicLong());
    this.maximumExecutions.get(pipelineName).set(maximumExecutions);
  }

  public ProcessRunnerStats getStats(String pipelineName) {
    stats.putIfAbsent(pipelineName, new ProcessRunnerStats(pipelineName, meterRegistry));
    return stats.get(pipelineName);
  }

  private void purgeStats() {
    for (ProcessRunnerStats stats : stats.values()) {
      stats.purge();
    }
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
