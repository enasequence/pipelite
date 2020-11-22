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
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;
import pipelite.configuration.LauncherConfiguration;
import pipelite.configuration.StageConfiguration;
import pipelite.cron.CronUtils;
import pipelite.entity.ProcessEntity;
import pipelite.entity.ScheduleEntity;
import pipelite.launcher.locker.LauncherLocker;
import pipelite.launcher.locker.ProcessLocker;
import pipelite.log.LogKey;
import pipelite.process.Process;
import pipelite.process.ProcessFactory;
import pipelite.process.ProcessState;
import pipelite.service.*;

@Flogger
@Component
@Scope("prototype")
public class PipeliteScheduler extends AbstractScheduledService {

  private final LauncherConfiguration launcherConfiguration;
  private final StageConfiguration stageConfiguration;
  private final ProcessFactoryService processFactoryService;
  private final ScheduleService scheduleService;
  private final ProcessService processService;
  private final StageService stageService;
  private final LockService lockService;
  private final String launcherName;
  private final LauncherLocker launcherLocker;
  private final ProcessLocker processLocker;
  private final ExecutorService executorService;

  private final Map<String, ProcessFactory> processFactoryCache = new ConcurrentHashMap<>();
  private final Set<String> activePipelines = new ConcurrentHashMap<>().newKeySet();
  private final Map<String, PipeliteSchedulerStats> stats = new ConcurrentHashMap<>();
  private final Map<String, AtomicLong> remainingExecutions = new ConcurrentHashMap<>();

  private final LocalDateTime startTime;

  private boolean shutdownAfterTriggered = false;

  @Data
  private static class Schedule {
    private ScheduleEntity scheduleEntity;
    private ProcessEntity processEntity;
    private Process process;
    private LocalDateTime launchTime;
  }

  private final List<Schedule> schedules = Collections.synchronizedList(new ArrayList<>());
  private LocalDateTime schedulesValidUntil;

  private final Duration processLaunchFrequency;
  private final Duration processRefreshFrequency;

  public PipeliteScheduler(
      @Autowired LauncherConfiguration launcherConfiguration,
      @Autowired StageConfiguration stageConfiguration,
      @Autowired ProcessFactoryService processFactoryService,
      @Autowired ScheduleService scheduleService,
      @Autowired ProcessService processService,
      @Autowired StageService stageService,
      @Autowired LockService lockService) {
    this.launcherConfiguration = launcherConfiguration;
    this.stageConfiguration = stageConfiguration;
    this.processFactoryService = processFactoryService;
    this.scheduleService = scheduleService;
    this.processService = processService;
    this.stageService = stageService;
    this.lockService = lockService;
    this.launcherName =
        LauncherConfiguration.getLauncherNameForPipeliteScheduler(launcherConfiguration);
    this.launcherLocker = new LauncherLocker(launcherName, lockService);
    this.processLocker = new ProcessLocker(launcherName, lockService);
    this.executorService = Executors.newCachedThreadPool();

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

    this.startTime = LocalDateTime.now();
  }

  @Override
  public String serviceName() {
    return launcherName;
  }

  @Override
  protected void startUp() {
    logContext(log.atInfo()).log("Starting up scheduler");
    if (!launcherLocker.lock()) {
      throw new RuntimeException("Could not start scheduler");
    }
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

    if (schedules.isEmpty() || schedulesValidUntil.isBefore(LocalDateTime.now())) {
      scheduleProcesses();
    }

    for (Schedule schedule : schedules) {
      String pipelineName = schedule.getScheduleEntity().getPipelineName();
      String cronExpression = schedule.getScheduleEntity().getSchedule();
      LocalDateTime launchTime = schedule.getLaunchTime();

      if (!activePipelines.contains(pipelineName) && launchTime.isBefore(LocalDateTime.now())) {

        if (remainingExecutions.get(pipelineName) != null
            && remainingExecutions.get(pipelineName).decrementAndGet() < 0) {
          continue;
        }

        // Update next launch time.
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

    logContext(log.atInfo()).log("Stopping pipelite scheduler after maximum executions");
    shutdownAfterTriggered = true;
    stopAsync();
  }

  private void scheduleProcesses() {

    logContext(log.atInfo()).log("Scheduling processes");

    schedules.clear();

    List<ScheduleEntity> scheduleEntities = scheduleService.getAllProcessSchedules(launcherName);
    logContext(log.atInfo()).log("Found %s schedules", scheduleEntities.size());

    for (ScheduleEntity scheduleEntity : scheduleEntities) {
      String scheduleDescription = "invalid cron expression";
      if (CronUtils.validate(scheduleEntity.getSchedule())) {
        Schedule schedule = new Schedule();
        schedule.setScheduleEntity(scheduleEntity);
        // Update next launch time.
        schedule.setLaunchTime(CronUtils.launchTime(scheduleEntity.getSchedule()));
        schedules.add(schedule);
        scheduleDescription = CronUtils.describe(scheduleEntity.getSchedule());
        logContext(log.atInfo(), scheduleEntity.getPipelineName())
            .log(
                "Scheduling %s pipeline with cron expression %s (%s). Next launch time is: %s",
                scheduleEntity.getPipelineName(),
                scheduleEntity.getSchedule(),
                scheduleDescription,
                schedule.getLaunchTime());
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

    int remainingProcessIdRetries = 100;
    while (true) {
      Optional<ProcessEntity> savedProcessEntity =
          processService.getSavedProcess(pipelineName, processId);
      if (savedProcessEntity.isPresent()) {
        processId = getNextProcessId(savedProcessEntity.get().getProcessId());
      } else {
        break;
      }
      if (--remainingProcessIdRetries <= 0) {
        throw new RuntimeException("Could not assign new process id");
      }
    }

    logContext(log.atInfo(), pipelineName, processId).log("Creating new process");

    ProcessFactory processFactory = getCachedProcessFactory(pipelineName);

    Process process = processFactory.create(processId);

    if (process == null) {
      logContext(log.atSevere(), processId).log("Failed to create process: %s", processId);
      setStats(pipelineName).processCreationFailedCount.incrementAndGet();
      return false;
    }

    ProcessEntity newProcessEntity = ProcessEntity.createExecution(pipelineName, processId, 9);
    processService.saveProcess(newProcessEntity);

    schedule.setProcess(process);
    schedule.setProcessEntity(newProcessEntity);

    schedule.getScheduleEntity().startExecution(processId);
    scheduleService.saveProcessSchedule(schedule.getScheduleEntity());

    return true;
  }

  private void launchProcess(Schedule schedule) {
    Process process = schedule.getProcess();
    ProcessEntity processEntity = schedule.getProcessEntity();
    String pipelineName = processEntity.getPipelineName();
    String processId = processEntity.getProcessId();

    logContext(log.atInfo(), pipelineName, processId).log("Launching process");

    ProcessLauncher processLauncher =
        new ProcessLauncher(
            launcherConfiguration,
            stageConfiguration,
            processService,
            stageService,
            pipelineName,
            process,
            processEntity);

    executorService.execute(
        () -> {
          activePipelines.add(pipelineName);
          try {
            if (!processLocker.lock(pipelineName, processId)) {
              return;
            }
            ProcessState state = processLauncher.run();
            setStats(pipelineName).setProcessExecutionCount(state).incrementAndGet();
          } catch (Exception ex) {
            setStats(pipelineName).processExceptionCount.incrementAndGet();
            logContext(log.atSevere(), pipelineName, processId)
                .withCause(ex)
                .log("Failed to execute process because an exception was thrown");
          } finally {
            schedule.getScheduleEntity().endExecution();
            scheduleService.saveProcessSchedule(schedule.getScheduleEntity());
            processLocker.unlock(pipelineName, processId);
            activePipelines.remove(pipelineName);
            setStats(pipelineName)
                .stageSuccessCount
                .addAndGet(processLauncher.getStageSuccessCount());
            setStats(pipelineName)
                .stageFailedCount
                .addAndGet(processLauncher.getStageFailedCount());
          }
        });
  }

  @Override
  protected void shutDown() throws Exception {
    logContext(log.atInfo()).log("Shutting down scheduler");

    executorService.shutdown();
    try {
      executorService.awaitTermination(ServerManager.FORCE_STOP_WAIT_SECONDS - 1, TimeUnit.SECONDS);
    } catch (InterruptedException ex) {
      executorService.shutdownNow();
      throw ex;
    } finally {
      launcherLocker.unlock();

      logContext(log.atInfo()).log("Scheduler has been shut down");
    }
  }

  public void removeLocks() {
    launcherLocker.removeLocks();
  }

  private PipeliteSchedulerStats setStats(String pipelineName) {
    stats.putIfAbsent(pipelineName, new PipeliteSchedulerStats());
    return stats.get(pipelineName);
  }

  public PipeliteSchedulerStats getStats(String pipelineName) {
    return stats.get(pipelineName);
  }

  public int getActivePipelinesCount() {
    return activePipelines.size();
  }

  public void setMaximumExecutions(String pipelineName, long maximumExecutions) {
    this.remainingExecutions.putIfAbsent(pipelineName, new AtomicLong());
    this.remainingExecutions.get(pipelineName).set(maximumExecutions);
  }

  private FluentLogger.Api logContext(FluentLogger.Api log) {
    return log.with(LogKey.LAUNCHER_NAME, launcherName);
  }

  private FluentLogger.Api logContext(FluentLogger.Api log, String pipelineName) {
    return log.with(LogKey.LAUNCHER_NAME, launcherName).with(LogKey.PIPELINE_NAME, pipelineName);
  }

  private FluentLogger.Api logContext(FluentLogger.Api log, String pipelineName, String processId) {
    return log.with(LogKey.LAUNCHER_NAME, launcherName)
        .with(LogKey.PIPELINE_NAME, pipelineName)
        .with(LogKey.PROCESS_ID, processId);
  }
}
