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
import java.util.concurrent.atomic.AtomicInteger;
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
import pipelite.service.LockService;
import pipelite.service.ProcessService;
import pipelite.service.ScheduleService;
import pipelite.service.StageService;

@Flogger
@Component
@Scope("prototype")
public class PipeliteScheduler extends AbstractScheduledService {

  private final LauncherConfiguration launcherConfiguration;
  private final StageConfiguration stageConfiguration;
  private final ScheduleService scheduleService;
  private final ProcessService processService;
  private final StageService stageService;
  private final LockService lockService;
  private final String launcherName;
  private final LauncherLocker launcherLocker;
  private final ProcessLocker processLocker;
  private final ExecutorService executorService;

  private final AtomicInteger processFailedToCreateCount = new AtomicInteger(0);
  private final AtomicInteger processExceptionCount = new AtomicInteger(0);
  private final AtomicInteger processCompletedCount = new AtomicInteger(0);
  private final AtomicInteger stageFailedCount = new AtomicInteger(0);
  private final AtomicInteger stageCompletedCount = new AtomicInteger(0);

  private final Map<String, ProcessFactory> processFactoryCache = new ConcurrentHashMap<>();

  private final Map<String, Schedule> activeProcesses = new ConcurrentHashMap<>();

  private final LocalDateTime startTime;
  private Duration shutdownAfter;
  private boolean shutdownAfterTriggered = false;

  @Data
  private static class Schedule {
    private ScheduleEntity scheduleEntity;
    private ProcessEntity processEntity;
    private Process process;
    private LocalDateTime launchTime;
  }

  private final ArrayList<Schedule> schedules = new ArrayList<>();
  private LocalDateTime schedulesValidUntil;

  private final Duration processLaunchFrequency;
  private final Duration processRefreshFrequency;

  public PipeliteScheduler(
      @Autowired LauncherConfiguration launcherConfiguration,
      @Autowired StageConfiguration stageConfiguration,
      @Autowired ScheduleService scheduleService,
      @Autowired ProcessService processService,
      @Autowired StageService stageService,
      @Autowired LockService lockService) {
    launcherConfiguration.validate();
    this.launcherConfiguration = launcherConfiguration;
    this.stageConfiguration = stageConfiguration;
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
      if (!activeProcesses.containsKey(schedule.getScheduleEntity().getPipelineName())
          && schedule.launchTime.isBefore(LocalDateTime.now())) {
        // Update next launch time.
        schedule.setLaunchTime(CronUtils.launchTime(schedule.getScheduleEntity().getSchedule()));
        logContext(log.atInfo(), schedule.getScheduleEntity().getPipelineName())
            .log(
                "Launching %s pipeline with cron expression %s (%s). Next launch time is: %s",
                schedule.getScheduleEntity().getPipelineName(),
                schedule.getScheduleEntity().getSchedule(),
                schedule.getScheduleEntity().getDescription(),
                schedule.getLaunchTime());
        if (createProcess(schedule)) {
          launchProcess(schedule);
        }
      }
    }

    shutdownIfAfter();
  }

  private void shutdownIfAfter() {
    if (startTime.plus(shutdownAfter).isBefore(LocalDateTime.now())) {
      logContext(log.atInfo())
          .log("Stopping pipelite scheduler after: " + shutdownAfter.toString());
      shutdownAfterTriggered = true;
      stopAsync();
    }
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

  private ProcessFactory getCachedProcessFactory(String processFactoryName) {
    if (processFactoryCache.containsKey(processFactoryName)) {
      return processFactoryCache.get(processFactoryName);
    }
    ProcessFactory processFactory = ProcessFactory.getProcessFactory(processFactoryName);
    processFactoryCache.put(processFactoryName, processFactory);
    return processFactory;
  }

  private boolean createProcess(Schedule schedule) {

    String pipelineName = schedule.getScheduleEntity().getPipelineName();

    String processId = getNextProcessId(schedule.getScheduleEntity().getProcessId());

    logContext(log.atInfo(), pipelineName, processId).log("Creating new process");

    ProcessFactory processFactory =
        getCachedProcessFactory(schedule.getScheduleEntity().getProcessFactoryName());

    Process process = processFactory.create(processId);

    if (process == null) {
      logContext(log.atSevere(), pipelineName, processId).log("Could not create process");
      processFailedToCreateCount.incrementAndGet();
      return false;
    }

    if (!validateProcess(schedule, process)) {
      logContext(log.atSevere(), pipelineName, processId).log("Failed to validate process");
      processFailedToCreateCount.incrementAndGet();
      return false;
    }

    Optional<ProcessEntity> savedProcessEntity =
        processService.getSavedProcess(pipelineName, processId);

    if (savedProcessEntity.isPresent()) {
      logContext(log.atSevere(), pipelineName, processId)
          .log("Could not create process because process id already exists");
      processFailedToCreateCount.incrementAndGet();
      return false;
    }

    ProcessEntity newProcessEntity = ProcessEntity.newExecution(processId, pipelineName, 9);
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
            launcherConfiguration, stageConfiguration, processService, stageService);

    processLauncher.init(process, processEntity);

    executorService.execute(
        () -> {
          activeProcesses.put(processId, schedule);
          try {
            if (!processLocker.lock(schedule.getScheduleEntity().getPipelineName(), processId)) {
              return;
            }
            processLauncher.run();
            processCompletedCount.incrementAndGet();
          } catch (Exception ex) {
            processExceptionCount.incrementAndGet();
            logContext(log.atSevere(), pipelineName, processId)
                .withCause(ex)
                .log("Failed to execute process");
          } finally {
            schedule.getScheduleEntity().endExecution();
            scheduleService.saveProcessSchedule(schedule.getScheduleEntity());
            processLocker.unlock(schedule.getScheduleEntity().getPipelineName(), processId);
            activeProcesses.remove(processId);
            stageCompletedCount.addAndGet(processLauncher.getStageCompletedCount());
            stageFailedCount.addAndGet(processLauncher.getStageFailedCount());
          }
        });
  }

  private boolean validateProcess(Schedule schedule, Process process) {
    if (process == null) {
      return false;
    }

    boolean isSuccess = process.validate(Process.ValidateMode.WITHOUT_STAGES);

    String pipelineName = schedule.getScheduleEntity().getPipelineName();

    if (!pipelineName.equals(process.getPipelineName())) {
      process
          .logContext(log.atSevere())
          .log("Pipeline name is different from launcher pipeline name: %s", pipelineName);
      isSuccess = false;
    }

    return isSuccess;
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

  public int getActiveProcessCount() {
    return activeProcesses.size();
  }

  public int getProcessFailedToCreateCount() {
    return processFailedToCreateCount.get();
  }

  public int getProcessExceptionCount() {
    return processExceptionCount.get();
  }

  public int getProcessCompletedCount() {
    return processCompletedCount.get();
  }

  public int getStageFailedCount() {
    return stageFailedCount.get();
  }

  public int getStageCompletedCount() {
    return stageCompletedCount.get();
  }

  public Duration getShutdownAfter() {
    return shutdownAfter;
  }

  public void setShutdownAfter(Duration shutdownAfter) {
    this.shutdownAfter = shutdownAfter;
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
