/*
 * Copyright 2020-2022 EMBL - European Bioinformatics Institute
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
import java.util.concurrent.atomic.AtomicReference;
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
import pipelite.exception.PipeliteUnrecoverableException;
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
    // Unexpected exceptions are logged as internal errors but otherwise ignored to
    // keep schedule runner alive.
    internalErrorHandler.execute(() -> runScheduleRunner());
  }

  private void runScheduleRunner() {
    if (!healthCheckService.isHealthy()) {
      logContext(log.atSevere())
          .log("Waiting data source to be healthy before starting new schedules");
      return;
    }
    startExecutions();
    // Must call ProcessRunnerPool.runOneIteration()
    super.runOneIteration();
  }

  private static String serviceName(PipeliteConfiguration pipeliteConfiguration) {
    return pipeliteConfiguration.service().getName() + "@scheduler";
  }

  @Override
  public void startUp() {
    super.startUp();
    scheduleExecutions();
    resumeExecutions();
  }

  @Override
  public boolean isIdle() {
    return !(maximumExecutions.isEmpty()
            || maximumExecutions.values().stream().anyMatch(r -> r.get() > 0))
        && super.isIdle();
  }

  /** Sets the next schedule execution times. */
  private void scheduleExecutions() {
    logContext(log.atInfo()).log("Scheduling executions");
    for (ScheduleCron scheduleCron : scheduleCrons) {
      // Unexpected exceptions are logged as internal errors but otherwise ignored to
      // not affect other schedules.
      internalErrorHandler.execute(
          () -> {
            Optional<ScheduleEntity> scheduleEntity =
                scheduleService.getSavedSchedule(scheduleCron.getPipelineName());
            if (!scheduleEntity.isPresent()) {
              throw new PipeliteException("Missing schedule: " + scheduleCron.getPipelineName());
            }
            scheduleExecution(scheduleCron, scheduleEntity.get());
          });
    }
  }

  /**
   * Sets the next schedule execution time.
   *
   * @param scheduleCron the schedule cron
   * @param scheduleEntity the schedule entity
   */
  private void scheduleExecution(ScheduleCron scheduleCron, ScheduleEntity scheduleEntity) {
    scheduleCron.setCron(scheduleEntity.getCron());
    if (!scheduleEntity.isActive() && scheduleEntity.getNextTime() == null) {
      logContext(log.atInfo(), scheduleEntity.getPipelineName())
          .log("Scheduling schedule execution");
      scheduleService.scheduleExecution(scheduleEntity);
    }
    scheduleCron.setNextTime(scheduleEntity.getNextTime());
  }

  /** Resumes schedule executions. */
  protected void resumeExecutions() {
    logContext(log.atInfo()).log("Resuming executions");
    // Unexpected exceptions are logged as internal errors but otherwise ignored to
    // not affect other schedules.
    getScheduleCrons().forEach(s -> internalErrorHandler.execute(() -> resumeExecution(s)));
  }

  /** Resumes schedule execution. */
  protected void resumeExecution(ScheduleCron scheduleCron) {
    String pipelineName = scheduleCron.getPipelineName();
    Optional<ScheduleEntity> scheduleEntity = scheduleService.getSavedSchedule(pipelineName);
    if (!scheduleEntity.isPresent()) {
      throw new PipeliteException("Missing schedule: " + scheduleCron.getPipelineName());
    }
    if (!scheduleEntity.get().isActive()) {
      return;
    }
    logContext(log.atInfo(), pipelineName).log("Resuming schedule execution");
    executeSchedule(scheduleCron, ExecuteMode.RESUME);
  }

  /** Starts new schedule executions. */
  protected void startExecutions() {
    // Unexpected exceptions are logged as internal errors but otherwise ignored to
    // not affect other schedules.
    getExecutableSchedules()
        .forEach(s -> internalErrorHandler.execute(() -> executeSchedule(s, ExecuteMode.NEW)));
  }

  /** Starts a new schedule execution. Called by web API. */
  public void startSchedule(String pipelineName) {
    Optional<ScheduleCron> scheduleCron =
        scheduleCrons.stream().filter(s -> s.getPipelineName().equals(pipelineName)).findAny();
    if (!scheduleCron.isPresent()) {
      throw new PipeliteUnrecoverableException("Missing schedule: " + pipelineName);
    }
    if (!scheduleCron.get().isExecutable()) {
      scheduleCron.get().setNextTime(ZonedDateTime.now());
    }
  }

  /** Retries a new schedule execution. Called by web API. */
  public void retrySchedule(String pipelineName, String processId) {
    Optional<ScheduleCron> scheduleCron =
        scheduleCrons.stream().filter(s -> s.getPipelineName().equals(pipelineName)).findAny();
    if (!scheduleCron.isPresent()) {
      throw new PipeliteProcessRetryException(pipelineName, processId, "unknown schedule");
    }
    executeSchedule(scheduleCron.get(), ExecuteMode.RETRY);
  }

  private Optional<ProcessEntity> getSavedProcess(ScheduleEntity scheduleEntity) {
    return processService.getSavedProcess(
        scheduleEntity.getPipelineName(), scheduleEntity.getProcessId());
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

  /** Executes a schedule. Creates a new process if needed. */
  protected synchronized void executeSchedule(ScheduleCron scheduleCron, ExecuteMode executeMode) {
    String pipelineName = scheduleCron.getPipelineName();
    logContext(log.atInfo(), pipelineName).log("Executing schedule");

    Schedule schedule = getSchedule(pipelineName);
    if (schedule == null) {
      return;
    }

    AtomicReference<ScheduleEntity> scheduleEntity =
        new AtomicReference<>(scheduleService.getSavedSchedule(pipelineName).orElse(null));
    if (executeMode == ExecuteMode.NEW) {
      scheduleEntity.set(executeNewSchedule(scheduleEntity.get()));
    }
    if (executeMode == ExecuteMode.RETRY) {
      scheduleEntity.set(executeRetrySchedule(scheduleEntity.get()));
    }

    AtomicReference<ProcessEntity> processEntity =
        new AtomicReference<>(getSavedProcess(scheduleEntity.get()).orElse(null));
    if (processEntity.get() == null) {
      // Create process for the schedule execution.
      processEntity.set(
          processService.createExecution(
              pipelineName, scheduleEntity.get().getProcessId(), ProcessEntity.DEFAULT_PRIORITY));
    }

    Process process = getProcess(processEntity.get(), schedule);
    if (process == null) {
      return;
    }

    setMaximumRetries(process);

    // Remove next time to prevent the schedule from being executed again until it
    // has completed.
    scheduleCron.setNextTime(null);

    runProcess(
        pipelineName,
        process,
        (p) -> {
          ZonedDateTime nextLaunchTime =
              CronUtils.launchTime(scheduleCron.getCron(), scheduleEntity.get().getStartTime());
          try {
            scheduleService.endExecution(processEntity.get(), nextLaunchTime);
          } finally {
            scheduleCron.setNextTime(nextLaunchTime);
            decreaseMaximumExecutions(pipelineName);
          }
        });
  }

  private ScheduleEntity executeNewSchedule(ScheduleEntity scheduleEntity) {
    String pipelineName = scheduleEntity.getPipelineName();
    String nextProcessId = nextProcessId(scheduleEntity.getProcessId());
    if (processService.getSavedProcess(pipelineName, nextProcessId).isPresent()) {
      throw new PipeliteException(
          "Failed to execute new "
              + pipelineName
              + " schedule. The "
              + nextProcessId
              + " process already exists.");
    }
    return scheduleService.startExecution(pipelineName, nextProcessId);
  }

  private ScheduleEntity executeRetrySchedule(ScheduleEntity scheduleEntity) {
    String pipelineName = scheduleEntity.getPipelineName();
    return scheduleService.startExecution(pipelineName, scheduleEntity.getProcessId());
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
