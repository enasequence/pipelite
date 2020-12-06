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
import java.time.ZonedDateTime;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import java.util.stream.Stream;
import lombok.extern.flogger.Flogger;
import org.springframework.util.Assert;
import pipelite.configuration.LauncherConfiguration;
import pipelite.cron.CronUtils;
import pipelite.entity.ProcessEntity;
import pipelite.entity.ScheduleEntity;
import pipelite.launcher.process.runner.ProcessRunnerPool;
import pipelite.launcher.process.runner.ProcessRunnerPoolService;
import pipelite.lock.PipeliteLocker;
import pipelite.log.LogKey;
import pipelite.process.Process;
import pipelite.process.ProcessFactory;
import pipelite.process.ProcessFactoryCache;
import pipelite.service.*;

@Flogger
public class PipeliteScheduler extends ProcessRunnerPoolService {

  private final ScheduleService scheduleService;
  private final ProcessService processService;
  private final ProcessFactoryCache processFactoryCache;
  private final List<Schedule> schedules = Collections.synchronizedList(new ArrayList<>());
  private final Set<Schedule> runningSchedules = ConcurrentHashMap.newKeySet();
  private final Map<String, AtomicLong> maximumExecutions = new ConcurrentHashMap<>();
  private final Map<String, ProcessLauncherStats> stats = new ConcurrentHashMap<>();
  private final Duration scheduleRefreshFrequency;
  private ZonedDateTime scheduleValidUntil = ZonedDateTime.now();

  public static class Schedule {
    private final ScheduleEntity scheduleEntity;
    private ZonedDateTime launchTime;

    public Schedule(ScheduleEntity scheduleEntity) {
      Assert.notNull(scheduleEntity, "Missing schedule entity");
      this.scheduleEntity = scheduleEntity;
    }

    public void scheduleExecution() {
      launchTime = CronUtils.launchTime(scheduleEntity.getCron());
    }

    public void resumeExecution() {
      launchTime = scheduleEntity.getStartTime();
    }

    public void endExecution() {
      launchTime = null;
    }

    public void refreshSchedule(ScheduleEntity newScheduleEntity) {
      scheduleEntity.setCron(newScheduleEntity.getCron());
      scheduleEntity.setActive(newScheduleEntity.getActive());
    }

    public void removeSchedule() {
      scheduleEntity.setActive(false);
    }

    public ScheduleEntity getScheduleEntity() {
      return scheduleEntity;
    }

    public ZonedDateTime getLaunchTime() {
      return launchTime;
    }

    public String getPipelineName() {
      return scheduleEntity.getPipelineName();
    }

    public boolean isActive() {
      return scheduleEntity.getActive() == null || scheduleEntity.getActive();
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      Schedule schedule = (Schedule) o;
      return scheduleEntity.getPipelineName().equals(schedule.scheduleEntity.getPipelineName());
    }

    @Override
    public int hashCode() {
      return scheduleEntity.getPipelineName().hashCode();
    }
  }

  public PipeliteScheduler(
      LauncherConfiguration launcherConfiguration,
      PipeliteLocker pipeliteLocker,
      ProcessFactoryService processFactoryService,
      ScheduleService scheduleService,
      ProcessService processService,
      Supplier<ProcessRunnerPool> processRunnerPoolSupplier) {
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
  }

  @Override
  protected void startUp() {
    super.startUp();
    refreshSchedules();
    resumeProcesses();
    scheduleProcesses();
  }

  @Override
  protected void run() {
    if (refreshSchedules()) {
      refreshSchedules();
    }
    scheduleProcesses();
    executeSchedules();
  }

  private void executeSchedules() {
    getExecutableSchedules()
        .forEach(schedule -> runProcess(schedule, createNewProcessEntity(schedule)));
  }

  @Override
  protected boolean shutdownIfIdle() {
    return !(maximumExecutions.isEmpty()
        || maximumExecutions.values().stream().anyMatch(r -> r.get() > 0));
  }

  /**
   * Returns true the if the schedules are allowed be refreshed to match the latest information in
   * the database. The permitted frequency is defined by scheduleRefreshFrequency.
   *
   * @return true the if the schedules are allowed be refreshed to match the latest information in
   *     the database
   */
  public boolean isRefreshSchedules() {
    return !scheduleValidUntil.isAfter(ZonedDateTime.now());
  }

  /**
   * Refreshes schedules to match the latest information in the database. The permitted frequency is
   * defined by scheduleRefreshFrequency.
   *
   * @return true the if the schedules were refreshed
   */
  protected boolean refreshSchedules() {
    if (!isRefreshSchedules()) {
      return false;
    }
    logContext(log.atInfo()).log("Refreshing schedules");
    scheduleValidUntil = ZonedDateTime.now().plus(scheduleRefreshFrequency);

    Collection<ScheduleEntity> newScheduleEntities =
        scheduleService.getAllProcessSchedules(getLauncherName());
    for (ScheduleEntity newScheduleEntity : newScheduleEntities) {
      Optional<Schedule> schedule = findSchedule(newScheduleEntity.getPipelineName());
      if (schedule.isPresent()) {
        refreshSchedule(schedule.get(), newScheduleEntity);
      } else {
        createSchedule(newScheduleEntity);
      }
    }
    removeSchedules(newScheduleEntities);
    return true;
  }

  private Optional<Schedule> findSchedule(String pipelineName) {
    return schedules.stream()
        .filter(schedule -> schedule.getPipelineName().equals(pipelineName))
        .findFirst();
  }

  /**
   * Creates a new schedule.
   *
   * @param newScheduleEntity the new schedule
   */
  private void createSchedule(ScheduleEntity newScheduleEntity) {
    updateCron(newScheduleEntity);
    if (!CronUtils.validate(newScheduleEntity.getCron())) {
      logContext(log.atSevere(), newScheduleEntity.getPipelineName())
          .log("Invalid cron expression: %s", newScheduleEntity.getCron());

    } else {
      schedules.add(new Schedule(newScheduleEntity));
    }
  }

  /**
   * Refreshes the schedule.
   *
   * @param newScheduleEntity the new schedule
   */
  private void refreshSchedule(Schedule schedule, ScheduleEntity newScheduleEntity) {
    updateCron(newScheduleEntity);
    if (!CronUtils.validate(newScheduleEntity.getCron())) {
      logContext(log.atSevere(), newScheduleEntity.getPipelineName())
          .log("Invalid cron expression: %s", newScheduleEntity.getCron());
      schedule.removeSchedule();
    } else {
      schedule.refreshSchedule(newScheduleEntity);
    }
  }

  /**
   * Updates the cron description if it is null or has changed.
   *
   * @param scheduleEntity the schedule
   */
  private void updateCron(ScheduleEntity scheduleEntity) {
    String description = CronUtils.describe(scheduleEntity.getCron());
    if (!description.equals(scheduleEntity.getDescription())) {
      scheduleEntity.setDescription(description);
      scheduleService.saveProcessSchedule(scheduleEntity);
    }
  }

  /**
   * Removes deleted schedules.
   *
   * @param newScheduleEntities the new schedules
   */
  private void removeSchedules(Collection<ScheduleEntity> newScheduleEntities) {
    for (Schedule schedule : schedules) {
      if (!newScheduleEntities.stream()
          .filter(s -> s.getPipelineName().equals(schedule.getPipelineName()))
          .findAny()
          .isPresent()) {
        schedule.removeSchedule();
      }
    }
  }

  /**
   * Attempts to resume processes. It may be possible to continue executing asynchronous processes.
   *
   * @return the number of process resume attempts.
   */
  protected int resumeProcesses() {
    AtomicInteger resumedProcessesCount = new AtomicInteger();
    getActiveSchedules()
        .forEach(
            schedule -> {
              if (isResumeProcess(schedule)) {
                if (resumeProcess(schedule)) {
                  resumedProcessesCount.incrementAndGet();
                }
              }
            });
    return resumedProcessesCount.get();
  }

  /**
   * Returns true if process execution can be resumed.
   *
   * @param schedule the schedule
   * @return true if process execution can be resumed
   */
  protected boolean isResumeProcess(Schedule schedule) {
    ScheduleEntity scheduleEntity = schedule.getScheduleEntity();
    return (scheduleEntity.getStartTime() != null && scheduleEntity.getProcessId() != null);
  }

  /**
   * Resumes process execution.
   *
   * @param schedule the schedule
   * @return true if process execution was resumed
   */
  protected boolean resumeProcess(Schedule schedule) {
    if (!isResumeProcess(schedule)) {
      return false;
    }
    logContext(log.atInfo(), schedule.getPipelineName()).log("Resuming process execution");
    Optional<ProcessEntity> processEntity = getSavedProcessEntity(schedule);
    if (!processEntity.isPresent()) {
      logContext(log.atSevere()).log("Could not resume process because it does not exist");
      return false;
    }
    schedule.resumeExecution();
    runProcess(schedule, processEntity.get());
    return true;
  }

  private Optional<ProcessEntity> getSavedProcessEntity(Schedule schedule) {
    return processService.getSavedProcess(
        schedule.getPipelineName(), schedule.getScheduleEntity().getProcessId());
  }

  /** Schedules processes for execution. */
  protected void scheduleProcesses() {
    getActiveSchedules()
        .forEach(
            schedule -> {
              if (schedule.getLaunchTime() == null) {
                scheduleProcess(schedule);
              }
            });
  }

  /**
   * Schedules a process for execution.
   *
   * @param schedule the schedule
   */
  protected void scheduleProcess(Schedule schedule) {
    ScheduleEntity scheduleEntity = schedule.getScheduleEntity();
    logContext(log.atInfo(), scheduleEntity.getPipelineName())
        .log(
            "Scheduling process for execution: %s cron %s (%s)",
            schedule.getLaunchTime(), scheduleEntity.getCron(), scheduleEntity.getDescription());
    schedule.scheduleExecution();
  }

  /**
   * Creates a new process entity to be executed.
   *
   * @param schedule the schedule
   * @return the new process entity or null if it could not be created
   */
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
      throw new RuntimeException("Invalid process id " + lastProcessId);
    }
  }

  /**
   * Executes the scheduled process.
   *
   * @param schedule the schedule
   * @param processEntity the process
   * @return true if the process could be executed
   */
  private boolean runProcess(Schedule schedule, ProcessEntity processEntity) {
    ScheduleEntity scheduleEntity = schedule.getScheduleEntity();
    String pipelineName = scheduleEntity.getPipelineName();
    String processId = processEntity.getProcessId();
    logContext(log.atInfo(), pipelineName, processId).log("Executing scheduled process");

    Process process =
        ProcessFactory.create(processEntity, processFactoryCache.getProcessFactory(pipelineName));
    if (process == null) {
      getStats(pipelineName).addProcessCreationFailedCount(1);
      logContext(log.atSevere(), pipelineName, processId).log("Failed to create scheduled process");
      return false;
    }
    maximumExecutions.get(pipelineName).decrementAndGet();
    runningSchedules.add(schedule);

    scheduleService.startExecution(scheduleEntity, process.getProcessId());
    runProcess(
        pipelineName,
        process,
        (p, r) -> {
          scheduleService.endExecution(scheduleEntity);
          schedule.endExecution();
          getStats(pipelineName).add(p, r);
          runningSchedules.remove(schedule);
        });
    return true;
  }

  /**
   * Returns all schedules.
   *
   * @return all schedules.
   */
  public Stream<Schedule> getSchedules() {
    return this.schedules.stream();
  }

  /**
   * Returns all active schedules.
   *
   * @return all active schedules.
   */
  public Stream<Schedule> getActiveSchedules() {
    return getSchedules().filter(schedule -> schedule.isActive());
  }

  /**
   * Returns all schedules that are not running and are waiting to be executed later.
   *
   * @return all schedules that are not running and are waiting to be executed later.
   */
  public Stream<Schedule> getPendingSchedules() {
    return getActiveSchedules()
        // Must have launch time.
        .filter(schedule -> schedule.launchTime != null)
        // Must not be running.
        .filter(schedule -> !isPipelineActive(schedule.scheduleEntity.getPipelineName()))
        // Must not have exceeded maximum executions.
        .filter(
            schedule ->
                maximumExecutions.get(schedule.getPipelineName()) == null
                    || maximumExecutions.get(schedule.getPipelineName()).get() > 0);
  }

  /**
   * Returns all stages that are not running and can be executed immediately.
   *
   * @return all stages that are not running and can be executed immediately.
   */
  public Stream<Schedule> getExecutableSchedules() {
    return getPendingSchedules()
        .filter(schedule -> !schedule.getLaunchTime().isAfter(ZonedDateTime.now()));
  }

  /**
   * Returns all running schedules.
   *
   * @return all running schedules.
   */
  public Stream<Schedule> getRunningSchedules() {
    return this.runningSchedules.stream();
  }

  public void setMaximumExecutions(String pipelineName, long maximumExecutions) {
    this.maximumExecutions.putIfAbsent(pipelineName, new AtomicLong());
    this.maximumExecutions.get(pipelineName).set(maximumExecutions);
  }

  public ProcessLauncherStats getStats(String pipelineName) {
    stats.putIfAbsent(pipelineName, new ProcessLauncherStats());
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
