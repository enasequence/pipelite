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
package pipelite.runner.schedule;

import com.google.common.flogger.FluentLogger;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;
import lombok.extern.flogger.Flogger;
import org.springframework.util.Assert;
import pipelite.Schedule;
import pipelite.configuration.PipeliteConfiguration;
import pipelite.cron.CronUtils;
import pipelite.entity.ProcessEntity;
import pipelite.entity.ScheduleEntity;
import pipelite.error.InternalErrorHandler;
import pipelite.exception.PipeliteException;
import pipelite.exception.PipeliteProcessRetryException;
import pipelite.exception.PipeliteScheduleStartException;
import pipelite.log.LogKey;
import pipelite.metrics.PipeliteMetrics;
import pipelite.process.Process;
import pipelite.process.ProcessFactory;
import pipelite.runner.process.ProcessRunnerFactory;
import pipelite.runner.process.ProcessRunnerPool;
import pipelite.runner.stage.StageRunner;
import pipelite.service.DataSourceHealthCheckService;
import pipelite.service.PipeliteServices;
import pipelite.service.ProcessService;
import pipelite.service.ScheduleService;

/** Executes non-parallel processes using cron expressions. */
@Flogger
public class ScheduleRunner extends ProcessRunnerPool {

  private final ScheduleService scheduleService;
  private final ProcessService processService;
  private final DataSourceHealthCheckService healthCheckService;
  private final ScheduleCache scheduleCache;
  private final List<ScheduleCron> scheduleCrons = Collections.synchronizedList(new ArrayList<>());
  private final Map<String, AtomicLong> maximumExecutions = new ConcurrentHashMap<>();
  private final String serviceName;
  private final InternalErrorHandler internalErrorHandler;

  public ScheduleRunner(
      PipeliteConfiguration pipeliteConfiguration,
      PipeliteServices pipeliteServices,
      PipeliteMetrics pipeliteMetrics,
      List<Schedule> schedules,
      ProcessRunnerFactory processRunnerFactory) {
    super(
        pipeliteConfiguration,
        pipeliteServices,
        pipeliteMetrics,
        serviceName(pipeliteConfiguration),
        processRunnerFactory);
    Assert.notNull(pipeliteConfiguration, "Missing configuration");
    Assert.notNull(pipeliteServices, "Missing services");
    this.scheduleService = pipeliteServices.schedule();
    this.processService = pipeliteServices.process();
    this.healthCheckService = pipeliteServices.healthCheck();
    this.scheduleCache = new ScheduleCache(pipeliteServices.registeredPipeline());
    this.serviceName = pipeliteConfiguration.service().getName();
    this.internalErrorHandler =
        new InternalErrorHandler(pipeliteServices.internalError(), serviceName, this);
    schedules.forEach(s -> this.scheduleCrons.add(new ScheduleCron(s.pipelineName())));
  }

  // From AbstractScheduledService.
  @Override
  public void runOneIteration() {
    internalErrorHandler.execute(
        () -> {
          if (!healthCheckService.isHealthy()) {
            logContext(log.atSevere())
                .log("Waiting data source to be healthy before starting new schedules");
            return;
          }
          // Schedule execution errors should not affect other schedules.
          getExecutableSchedules()
              .forEach(
                  s ->
                      internalErrorHandler.execute(
                          () -> executeSchedule(s, createProcessEntity(s), ExecuteMode.NEW)));
          // Must call ProcessRunnerPool.runOneIteration()
          super.runOneIteration();
        });
  }

  private static String serviceName(PipeliteConfiguration pipeliteConfiguration) {
    return pipeliteConfiguration.service().getName() + "@scheduler";
  }

  @Override
  public void startUp() {
    super.startUp();
    initSchedules();
    resumeSchedules();
  }

  @Override
  public boolean isIdle() {
    return !(maximumExecutions.isEmpty()
            || maximumExecutions.values().stream().anyMatch(r -> r.get() > 0))
        && super.isIdle();
  }

  /** Initialises schedules. */
  private void initSchedules() {
    logContext(log.atInfo()).log("Initialising schedules");
    for (ScheduleCron scheduleCron : scheduleCrons) {
      Optional<ScheduleEntity> scheduleEntity =
          scheduleService.getSavedSchedule(scheduleCron.getPipelineName());
      if (!scheduleEntity.isPresent()) {
        throw new PipeliteException("Unknown schedule: " + scheduleCron.getPipelineName());
      }
      initSchedule(scheduleCron, scheduleEntity.get());
    }
  }

  /**
   * Initialises a schedule.
   *
   * @param scheduleCron the schedule cron
   * @param scheduleEntity the schedule entity
   */
  private void initSchedule(ScheduleCron scheduleCron, ScheduleEntity scheduleEntity) {
    scheduleCron.setCron(scheduleEntity.getCron());
    if (!scheduleEntity.isActive() && scheduleEntity.getNextTime() == null) {
      scheduleService.scheduleExecution(scheduleEntity);
    }
    scheduleCron.setLaunchTime(scheduleEntity.getNextTime());
  }

  protected void resumeSchedules() {
    getScheduleCrons()
        .forEach(
            s -> {
              try {
                resumeSchedule(s);
              } catch (Exception ex) {
                logContext(log.atSevere(), s.getPipelineName()).log("Could not resume schedule");
              }
            });
  }

  protected void resumeSchedule(ScheduleCron scheduleCron) {
    String pipelineName = scheduleCron.getPipelineName();
    ScheduleEntity scheduleEntity = scheduleService.getSavedSchedule(pipelineName).get();
    if (!scheduleEntity.isActive()) {
      return;
    }
    logContext(log.atInfo(), pipelineName).log("Resuming schedule");
    Optional<ProcessEntity> processEntity = getSavedProcess(scheduleEntity);
    if (!processEntity.isPresent()) {
      logContext(log.atSevere(), pipelineName, scheduleEntity.getProcessId())
          .log("Could not resume schedule because process does not exist");
    } else {
      executeSchedule(scheduleCron, processEntity.get(), ExecuteMode.RESUME);
    }
  }

  public void startSchedule(String pipelineName) {
    Optional<ScheduleCron> scheduleCron =
        scheduleCrons.stream().filter(s -> s.getPipelineName().equals(pipelineName)).findAny();
    if (!scheduleCron.isPresent()) {
      throw new PipeliteScheduleStartException(pipelineName, "unknown schedule");
    }
    if (!scheduleCron.get().isExecutable()) {
      scheduleCron.get().setLaunchTime(ZonedDateTime.now());
    } else {
      throw new PipeliteScheduleStartException(pipelineName, "schedule is already being started");
    }
  }

  public void retrySchedule(String pipelineName, String processId) {
    Optional<ScheduleCron> scheduleCron =
        scheduleCrons.stream().filter(s -> s.getPipelineName().equals(pipelineName)).findAny();
    if (!scheduleCron.isPresent()) {
      throw new PipeliteProcessRetryException(pipelineName, processId, "unknown schedule");
    }
    Optional<ProcessEntity> processEntity = processService.getSavedProcess(pipelineName, processId);
    if (!processEntity.isPresent()) {
      throw new PipeliteProcessRetryException(pipelineName, processId, "unknown process");
    }
    executeSchedule(scheduleCron.get(), processEntity.get(), ExecuteMode.RETRY);
  }

  public boolean isActiveSchedule(String pipelineName) {
    return getActiveProcessRunners().stream()
        .filter(p -> p.getPipelineName().equals(pipelineName))
        .findFirst()
        .isPresent();
  }

  private Optional<ProcessEntity> getSavedProcess(ScheduleEntity scheduleEntity) {
    return processService.getSavedProcess(
        scheduleEntity.getPipelineName(), scheduleEntity.getProcessId());
  }

  private ProcessEntity createProcessEntity(ScheduleCron scheduleCron) {
    String pipelineName = scheduleCron.getPipelineName();
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

  protected enum ExecuteMode {
    NEW,
    RESUME,
    RETRY
  };

  /** Executes a schedule expecting the process to already exist. */
  protected void executeSchedule(
      ScheduleCron scheduleCron, ProcessEntity processEntity, ExecuteMode executeMode) {
    internalErrorHandler.execute(
        () -> {
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

          if (executeMode != ExecuteMode.RESUME) {
            scheduleService.startExecution(pipelineName, processId);
          }

          scheduleCron.setLaunchTime(null);

          runProcess(
              pipelineName,
              process,
              (p) -> {
                ScheduleEntity scheduleEntity =
                    scheduleService.getSavedSchedule(pipelineName).get();
                ZonedDateTime nextLaunchTime =
                    CronUtils.launchTime(scheduleCron.getCron(), scheduleEntity.getStartTime());
                internalErrorHandler.execute(
                    () -> scheduleService.endExecution(processEntity, nextLaunchTime));
                scheduleCron.setLaunchTime(nextLaunchTime);
                decreaseMaximumExecutions(pipelineName);
              });
        });
  }

  private Schedule getSchedule(String pipelineName) {
    Schedule schedule = scheduleCache.getSchedule(pipelineName);
    if (schedule == null) {
      throw new PipeliteException("Failed to create a schedule for pipeline: " + pipelineName);
    }
    return schedule;
  }

  private Process getProcess(ProcessEntity processEntity, Schedule schedule) {
    return ProcessFactory.create(processEntity, schedule);
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
                    .setMaximumRetries(StageRunner.getImmediateRetries(stage)));
  }

  /**
   * Returns the schedules.
   *
   * @return the schedules
   */
  public List<ScheduleCron> getScheduleCrons() {
    return scheduleCrons;
  }

  /**
   * Returns schedules that can be executed.
   *
   * @return schedules that can be executed
   */
  protected Stream<ScheduleCron> getExecutableSchedules() {
    return getScheduleCrons().stream()
        // Must be executable.
        .filter(s -> s.isExecutable())
        // Must not be running.
        .filter(s -> !isPipelineActive(s.getPipelineName()))
        // Must not have exceeded maximum executions (not used in production).
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
    return log.with(LogKey.PROCESS_RUNNER_NAME, serviceName());
  }

  private FluentLogger.Api logContext(FluentLogger.Api log, String pipelineName) {
    return log.with(LogKey.PROCESS_RUNNER_NAME, serviceName())
        .with(LogKey.PIPELINE_NAME, pipelineName);
  }

  private FluentLogger.Api logContext(FluentLogger.Api log, String pipelineName, String processId) {
    return log.with(LogKey.PROCESS_RUNNER_NAME, serviceName())
        .with(LogKey.PIPELINE_NAME, pipelineName)
        .with(LogKey.PROCESS_ID, processId);
  }
}
