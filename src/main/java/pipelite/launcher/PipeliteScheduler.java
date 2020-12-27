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
import pipelite.launcher.process.runner.ProcessRunnerMetrics;
import pipelite.launcher.process.runner.ProcessRunnerPool;
import pipelite.launcher.process.runner.ProcessRunnerPoolService;
import pipelite.lock.PipeliteLocker;
import pipelite.log.LogKey;
import pipelite.process.Process;
import pipelite.process.ProcessFactory;
import pipelite.process.ProcessFactoryCache;
import pipelite.schedule.Schedule;
import pipelite.service.*;

/**
 * Schedules non-parallel processes using cron expressions. New process instances are created using
 * {@link pipelite.process.ProcessFactory}.
 */
@Flogger
public class PipeliteScheduler extends ProcessRunnerPoolService {

  private final ScheduleService scheduleService;
  private final ProcessService processService;
  private final ProcessFactoryCache processFactoryCache;
  private final List<Schedule> schedules = Collections.synchronizedList(new ArrayList<>());
  private final Map<String, AtomicLong> maximumExecutions = new ConcurrentHashMap<>();
  private final Map<String, ProcessRunnerMetrics> metrics = new ConcurrentHashMap<>();
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
    purgeMetrics();
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
   * Creates and enables the schedule.
   *
   * @param scheduleEntity the schedule entity
   */
  private void createSchedule(ScheduleEntity scheduleEntity) {
    String pipelineName = scheduleEntity.getPipelineName();
    updateCron(scheduleEntity);
    Schedule schedule = new Schedule(pipelineName);
    schedule.setCron(scheduleEntity.getCron());
    if (scheduleEntity.getNextTime() != null) {
      // Use previously assigned launch time. The launch time is removed when the process
      // execution starts and assigned when the process execution finishes or here if the launch
      // time has not been assigned.
      schedule.enable(scheduleEntity.getNextTime());
    } else {
      // Evaluate the cron expression and assign the next launch time.
      schedule.enable();
      scheduleService.scheduleExecution(pipelineName, schedule.getLaunchTime());
    }
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

  protected void resumeSchedules() {
    getSchedules()
        .forEach(
            s -> {
              try {
                resumeSchedule(s);
              } catch (Exception ex) {
                logContext(log.atSevere(), s.getPipelineName()).log("Could not resume schedule");
              }
            });
  }

  protected void resumeSchedule(Schedule schedule) {
    String pipelineName = schedule.getPipelineName();
    ScheduleEntity scheduleEntity = scheduleService.geSavedSchedule(pipelineName).get();
    if (!scheduleEntity.isResumeProcess()) {
      return;
    }
    logContext(log.atInfo(), pipelineName).log("Resuming schedule");
    Optional<ProcessEntity> processEntity = getSavedProcessEntity(scheduleEntity);
    if (!processEntity.isPresent()) {
      logContext(log.atSevere(), pipelineName, scheduleEntity.getProcessId())
          .log("Could not resume schedule because process does not exist");
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
   * @throws PipeliteException if a new process id could not be created
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
      getMetrics(pipelineName).addProcessCreationFailed(1);
      logContext(log.atSevere(), pipelineName, processId).log("Failed to create scheduled process");
      return;
    }
    scheduleService.startExecution(pipelineName, processId);
    schedule.disable();
    maximumExecutions.get(pipelineName).decrementAndGet();

    runProcess(
        pipelineName,
        process,
        (p, r) -> {
          scheduleService.endExecution(processEntity);
          schedule.enable();
          scheduleService.scheduleExecution(pipelineName, schedule.getLaunchTime());
          getMetrics(pipelineName).addProcessRunnerResult(p.getProcessEntity().getState(), r);
        });
  }

  /**
   * Returns the schedules.
   *
   * @return the schedules
   */
  public List<Schedule> getSchedules() {
    return schedules;
  }

  /**
   * Returns schedules that can be executed.
   *
   * @return schedules that can be executed
   */
  private Stream<Schedule> getExecutableSchedules() {
    return getSchedules().stream()
        // Must be executable.
        .filter(s -> s.isExecutable())
        // Must not be running.
        .filter(s -> !isPipelineActive(s.getPipelineName()))
        // Must not have exceeded maximum executions.
        .filter(
            s ->
                maximumExecutions.get(s.getPipelineName()) == null
                    || maximumExecutions.get(s.getPipelineName()).get() > 0);
  }

  public void setMaximumExecutions(String pipelineName, long maximumExecutions) {
    this.maximumExecutions.putIfAbsent(pipelineName, new AtomicLong());
    this.maximumExecutions.get(pipelineName).set(maximumExecutions);
  }

  public ProcessRunnerMetrics getMetrics(String pipelineName) {
    metrics.putIfAbsent(pipelineName, new ProcessRunnerMetrics(pipelineName, meterRegistry));
    return metrics.get(pipelineName);
  }

  private void purgeMetrics() {
    for (ProcessRunnerMetrics m : metrics.values()) {
      m.purge();
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
