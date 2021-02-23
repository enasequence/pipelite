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
import java.time.ZonedDateTime;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;
import lombok.extern.flogger.Flogger;
import org.springframework.util.Assert;
import pipelite.Pipeline;
import pipelite.Schedule;
import pipelite.entity.ProcessEntity;
import pipelite.entity.ScheduleEntity;
import pipelite.exception.PipeliteException;
import pipelite.launcher.process.runner.ProcessRunnerPool;
import pipelite.launcher.process.runner.ProcessRunnerPoolService;
import pipelite.log.LogKey;
import pipelite.process.Process;
import pipelite.process.ProcessFactory;
import pipelite.service.HealthCheckService;
import pipelite.service.InternalErrorService;
import pipelite.service.ProcessService;
import pipelite.service.ScheduleService;

/**
 * Schedules non-parallel processes using cron expressions. New process instances are created using
 * {@link Pipeline}.
 */
@Flogger
public class PipeliteScheduler extends ProcessRunnerPoolService {

  private final ScheduleService scheduleService;
  private final ProcessService processService;
  private final InternalErrorService internalErrorService;
  private final HealthCheckService healthCheckService;
  private final ScheduleCache scheduleCache;
  private final List<PipeliteSchedulerSchedule> schedules =
      Collections.synchronizedList(new ArrayList<>());
  private final Map<String, AtomicLong> maximumExecutions = new ConcurrentHashMap<>();
  private final String serviceName;

  public PipeliteScheduler(
      PipeliteConfiguration pipeliteConfiguration,
      PipeliteServices pipeliteServices,
      ProcessRunnerPool processRunnerPool,
      List<Schedule> schedules) {
    super(pipeliteConfiguration, processRunnerPool);
    Assert.notNull(pipeliteConfiguration, "Missing configuration");
    Assert.notNull(pipeliteServices, "Missing services");
    this.scheduleService = pipeliteServices.schedule();
    this.processService = pipeliteServices.process();
    this.internalErrorService = pipeliteServices.internalError();
    this.healthCheckService = pipeliteServices.healthCheckService();
    this.scheduleCache = new ScheduleCache(pipeliteServices.registeredPipeline());
    this.serviceName = pipeliteConfiguration.service().getName();
    schedules.forEach(s -> this.schedules.add(new PipeliteSchedulerSchedule(s.pipelineName())));
  }

  @Override
  public String getLauncherName() {
    return serviceName + "@scheduler";
  }

  @Override
  protected void startUp() {
    super.startUp();
    loadSchedules();
    resumeSchedules();
  }

  @Override
  protected void run() {
    executeSchedules();
  }

  protected void executeSchedules() {
    try {
      if (!healthCheckService.isDataSourceHealthy()) {
        logContext(log.atSevere())
            .log("Waiting data source to be healthy before starting new schedules");
        return;
      }
      getExecutableSchedules()
          .forEach(
              s -> {
                try {
                  executeSchedule(s, createProcessEntity(s), false);
                } catch (Exception ex) {
                  // Catching exceptions here to allow other schedules to continue execution.
                  internalErrorService.saveInternalError(
                      serviceName, s.getPipelineName(), this.getClass(), ex);
                }
              });

    } catch (Exception ex) {
      // Catching exceptions here in case they have not already been caught.
      internalErrorService.saveInternalError(serviceName, this.getClass(), ex);
    }
  }

  @Override
  protected boolean shutdownIfIdle() {
    return !(maximumExecutions.isEmpty()
        || maximumExecutions.values().stream().anyMatch(r -> r.get() > 0));
  }

  private void loadSchedules() {
    logContext(log.atInfo()).log("Loading schedules");
    for (PipeliteSchedulerSchedule schedule : schedules) {
      Optional<ScheduleEntity> scheduleEntity =
          scheduleService.getSavedSchedule(schedule.getPipelineName());
      if (scheduleEntity == null) {
        throw new PipeliteException("Unknown scheduled pipeline: " + schedule.getPipelineName());
      }
      createSchedule(schedule, scheduleEntity.get());
    }
  }

  /**
   * Creates and enables the schedule.
   *
   * @param schedule the schedule
   * @param scheduleEntity the schedule entity
   */
  private void createSchedule(PipeliteSchedulerSchedule schedule, ScheduleEntity scheduleEntity) {
    schedule.setCron(scheduleEntity.getCron());
    if (scheduleEntity.getNextTime() != null) {
      // Use previously assigned launch time. The launch time is removed when the process
      // execution starts and assigned when the process execution finishes or here if the launch
      // time has not been assigned.
      schedule.setLaunchTime(scheduleEntity.getNextTime());
    } else {
      // Evaluate the cron expression and assign the next launch time.
      schedule.setLaunchTime(schedule.getNextLaunchTime());
      scheduleService.scheduleExecution(schedule.getPipelineName(), schedule.getLaunchTime());
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

  protected void resumeSchedule(PipeliteSchedulerSchedule schedule) {
    String pipelineName = schedule.getPipelineName();
    ScheduleEntity scheduleEntity = scheduleService.getSavedSchedule(pipelineName).get();
    if (!scheduleEntity.isResumeProcess()) {
      return;
    }
    logContext(log.atInfo(), pipelineName).log("Resuming schedule");
    Optional<ProcessEntity> processEntity = getSavedProcessEntity(scheduleEntity);
    if (!processEntity.isPresent()) {
      logContext(log.atSevere(), pipelineName, scheduleEntity.getProcessId())
          .log("Could not resume schedule because process does not exist");
    } else {
      executeSchedule(schedule, processEntity.get(), true);
    }
  }

  private Optional<ProcessEntity> getSavedProcessEntity(ScheduleEntity scheduleEntity) {
    return processService.getSavedProcess(
        scheduleEntity.getPipelineName(), scheduleEntity.getProcessId());
  }

  private ProcessEntity createProcessEntity(PipeliteSchedulerSchedule schedule) {
    String pipelineName = schedule.getPipelineName();
    ScheduleEntity scheduleEntity = scheduleService.getSavedSchedule(pipelineName).get();

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
      return String.valueOf(Long.parseLong(lastProcessId) + 1);
    } catch (Exception ex) {
      throw new PipeliteException("Invalid process id " + lastProcessId);
    }
  }

  protected void executeSchedule(
      PipeliteSchedulerSchedule pipeliteSchedulerSchedule,
      ProcessEntity processEntity,
      boolean isResume) {
    String pipelineName = processEntity.getPipelineName();
    String processId = processEntity.getProcessId();
    logContext(log.atInfo(), pipelineName, processId).log("Executing scheduled process");

    Schedule schedule = getSchedule(pipelineName);
    if (schedule == null) {
      return;
    }

    Process process = getProcess(processEntity, schedule);
    if (process == null) {
      return;
    }

    setMaximumRetries(process);

    if (!isResume) {
      scheduleService.startExecution(pipelineName, processId);
    }

    pipeliteSchedulerSchedule.setLaunchTime(null);

    try {
      runProcess(
          pipelineName,
          process,
          (p, r) -> {
            ZonedDateTime nextLaunchTime = pipeliteSchedulerSchedule.getNextLaunchTime();
            try {
              scheduleService.endExecution(processEntity, nextLaunchTime);
            } catch (Exception ex) {
              internalErrorService.saveInternalError(
                  serviceName, pipelineName, this.getClass(), ex);
            } finally {
              pipeliteSchedulerSchedule.setLaunchTime(nextLaunchTime);
              decreaseMaximumExecutions(pipelineName);
            }
          });
    } catch (Exception ex) {
      internalErrorService.saveInternalError(serviceName, pipelineName, this.getClass(), ex);
    }
  }

  private Process getProcess(ProcessEntity processEntity, Schedule schedule) {
    return ProcessFactory.create(processEntity, schedule);
  }

  private Schedule getSchedule(String pipelineName) {
    Schedule schedule = scheduleCache.getSchedule(pipelineName);
    if (schedule == null) {
      throw new PipeliteException("Failed to create a schedule for pipeline: " + pipelineName);
    }
    return schedule;
  }

  /**
   * Scheduled pipelines support only immediate retries. Set the maximum retries to the number of
   * immediate retries.
   */
  private void setMaximumRetries(Process process) {
    process
        .getStages()
        .forEach(
            stage ->
                stage
                    .getExecutor()
                    .getExecutorParams()
                    .setMaximumRetries(StageLauncher.getImmediateRetries(stage)));
  }

  /**
   * Returns the schedules.
   *
   * @return the schedules
   */
  public List<PipeliteSchedulerSchedule> getSchedules() {
    return schedules;
  }

  /**
   * Returns schedules that can be executed.
   *
   * @return schedules that can be executed
   */
  protected Stream<PipeliteSchedulerSchedule> getExecutableSchedules() {
    return getSchedules().stream()
        // Must be executable.
        .filter(s -> s.isExecutable())
        // Must not be running.
        .filter(s -> !isPipelineActive(s.getPipelineName()))
        // Must not have exceeded maximum executions.
        .filter(
            s ->
                !maximumExecutions.containsKey(s.getPipelineName())
                    || maximumExecutions.get(s.getPipelineName()).get() > 0);
  }

  private void decreaseMaximumExecutions(String pipelineName) {
    if (maximumExecutions.containsKey(pipelineName)) {
      maximumExecutions.get(pipelineName).decrementAndGet();
    }
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
