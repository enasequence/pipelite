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
import lombok.Data;
import lombok.extern.flogger.Flogger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import pipelite.configuration.LauncherConfiguration;
import pipelite.configuration.TaskConfiguration;
import pipelite.cron.CronUtils;
import pipelite.entity.PipeliteSchedule;
import pipelite.entity.PipeliteProcess;
import pipelite.log.LogKey;
import pipelite.process.ProcessFactory;
import pipelite.process.ProcessInstance;
import pipelite.service.PipeliteScheduleService;
import pipelite.service.PipeliteLockService;
import pipelite.service.PipeliteProcessService;
import pipelite.service.PipeliteStageService;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

@Flogger
@Component
public class ScheduleLauncher extends AbstractScheduledService {

  private final LauncherConfiguration launcherConfiguration;
  private final TaskConfiguration taskConfiguration;
  private final PipeliteScheduleService pipeliteScheduleService;
  private final PipeliteProcessService pipeliteProcessService;
  private final PipeliteStageService pipeliteStageService;
  private final PipeliteLockService pipeliteLockService;
  private final String launcherName;
  private final LauncherLocker launcherLocker;
  private final ExecutorService executorService;
  private final int workers;

  private final AtomicInteger processCreateFailedCount = new AtomicInteger(0);
  private final AtomicInteger processStartFailedCount = new AtomicInteger(0);
  private final AtomicInteger processFailedCount = new AtomicInteger(0);
  private final AtomicInteger processCompletedCount = new AtomicInteger(0);
  private final AtomicInteger taskFailedCount = new AtomicInteger(0);
  private final AtomicInteger taskSkippedCount = new AtomicInteger(0);
  private final AtomicInteger taskCompletedCount = new AtomicInteger(0);

  private final Map<String, Schedule> initProcesses = new ConcurrentHashMap<>();
  private final Map<String, Schedule> activeProcesses = new ConcurrentHashMap<>();

  @Data
  private static class Schedule {
    private PipeliteSchedule pipeliteSchedule;
    private PipeliteProcess pipeliteProcess;
    private ProcessInstance processInstance;
    private LocalDateTime launchTime;
  }

  private final ArrayList<Schedule> schedules = new ArrayList<>();
  private LocalDateTime schedulesValidUntil = LocalDateTime.now();

  private long iterations = 0;
  private Long maxIterations;

  public static final Duration DEFAULT_RUN_DELAY = Duration.ofMinutes(1);
  public static final Duration DEFAULT_REFRESH_DELAY = Duration.ofHours(1);
  private final Duration runDelay;

  public ScheduleLauncher(
      @Autowired LauncherConfiguration launcherConfiguration,
      @Autowired TaskConfiguration taskConfiguration,
      @Autowired PipeliteScheduleService pipeliteScheduleService,
      @Autowired PipeliteProcessService pipeliteProcessService,
      @Autowired PipeliteStageService pipeliteStageService,
      @Autowired PipeliteLockService pipeliteLockService) {
    this.launcherConfiguration = launcherConfiguration;
    this.taskConfiguration = taskConfiguration;
    this.pipeliteScheduleService = pipeliteScheduleService;
    this.pipeliteProcessService = pipeliteProcessService;
    this.pipeliteStageService = pipeliteStageService;
    this.pipeliteLockService = pipeliteLockService;
    this.launcherName = launcherConfiguration.getLauncherName();
    this.launcherLocker = new LauncherLocker(launcherName, pipeliteLockService);
    this.workers =
        launcherConfiguration.getWorkers() > 0
            ? launcherConfiguration.getWorkers()
            : PipeliteLauncher.DEFAULT_WORKERS;
    this.executorService = Executors.newFixedThreadPool(workers);

    if (launcherConfiguration.getRunDelay() != null) {
      this.runDelay = launcherConfiguration.getRunDelay();
    } else {
      this.runDelay = DEFAULT_RUN_DELAY;
    }
  }

  @Override
  public String serviceName() {
    return launcherName;
  }

  @Override
  protected void startUp() {
    logContext(log.atInfo()).log("Starting up launcher");
    if (!launcherLocker.lockLauncher()) {
      throw new RuntimeException("Could not start launcher");
    }
  }

  @Override
  protected Scheduler scheduler() {
    return Scheduler.newFixedDelaySchedule(Duration.ZERO, runDelay);
  }

  @Override
  protected void runOneIteration() {
    if (!isRunning()) {
      return;
    }

    logContext(log.atInfo()).log("Running launcher");

    if (schedules.isEmpty() || schedulesValidUntil.isBefore(LocalDateTime.now())) {
      scheduleProcesses();
    }

    for (Schedule schedule : schedules) {
      if (!activeProcesses.containsKey(schedule.pipeliteProcess.getProcessName())
          && schedule.launchTime.isAfter(LocalDateTime.now())) {
        if (createProcess(schedule)) {
          launchProcess(schedule);
        }
      }
    }

    stopIfMaxIterations();
  }

  private void stopIfMaxIterations() {
    if (maxIterations != null && ++iterations > maxIterations) {
      stopAsync();
    }
  }

  private void scheduleProcesses() {

    logContext(log.atInfo()).log("Scheduling processes");

    schedules.clear();
    for (PipeliteSchedule pipeliteSchedule : pipeliteScheduleService.getAllProcessSchedules(launcherName)) {

      String scheduleDescription = "invalid cron expression";
      if (CronUtils.validate(pipeliteSchedule.getSchedule())) {
        Schedule schedule = new Schedule();
        schedule.setPipeliteSchedule(pipeliteSchedule);
        schedule.setLaunchTime(CronUtils.launchTime(pipeliteSchedule.getSchedule()));
        schedules.add(schedule);
        scheduleDescription = CronUtils.describe(pipeliteSchedule.getSchedule());
      }
      if (!scheduleDescription.equals(pipeliteSchedule.getDescription())) {
        pipeliteSchedule.setDescription(scheduleDescription);
        pipeliteScheduleService.saveProcessSchedule(pipeliteSchedule);
      }
    }
  }

  private boolean createProcess(Schedule schedule) {

    String processName = schedule.getPipeliteSchedule().getProcessName();
    String processId = String.valueOf(schedule.getPipeliteSchedule().getExecutionCount() + 1);

    logContext(log.atInfo(), processName, processId).log("Creating new scheduled process instance");

    ProcessFactory processFactory =
        ProcessFactory.getProcessFactory(schedule.getPipeliteSchedule().getProcessFactoryName());

    ProcessInstance processInstance = processFactory.create(processId);

    if (processInstance == null) {
      logContext(log.atSevere(), processName, processId).log("Could not create process instance");
      processCreateFailedCount.incrementAndGet();
      return false;
    }

    if (!validateProcess(schedule, processInstance)) {
      logContext(log.atSevere(), processName, processId).log("Failed to validate process instance");
      processCreateFailedCount.incrementAndGet();
      return false;
    }

    Optional<PipeliteProcess> savedPipeliteProcess =
        pipeliteProcessService.getSavedProcess(processName, processId);

    if (savedPipeliteProcess.isPresent()) {
      logContext(log.atSevere(), processName, processId)
          .log("Could not create process instance because process id already exists");
      processCreateFailedCount.incrementAndGet();
      return false;
    }

    PipeliteProcess newPipeliteProcess = PipeliteProcess.newExecution(processId, processName, 9);
    pipeliteProcessService.saveProcess(newPipeliteProcess);

    schedule.setProcessInstance(processInstance);
    schedule.setPipeliteProcess(newPipeliteProcess);

    schedule.getPipeliteSchedule().startExecution();
    pipeliteScheduleService.saveProcessSchedule(schedule.getPipeliteSchedule());

    return true;
  }

  private void launchProcess(Schedule schedule) {
    ProcessInstance processInstance = schedule.getProcessInstance();
    PipeliteProcess pipeliteProcess = schedule.getPipeliteProcess();
    String processName = pipeliteProcess.getProcessName();
    String processId = pipeliteProcess.getProcessId();

    logContext(log.atInfo(), processName, processId).log("Launching process");

    ProcessLauncher processLauncher =
        new ProcessLauncher(
            launcherConfiguration, taskConfiguration, pipeliteProcessService, pipeliteStageService);

    processLauncher.init(processInstance, pipeliteProcess);
    initProcesses.put(processId, schedule);

    executorService.execute(
        () -> {
          activeProcesses.put(processId, schedule);
          try {
            if (!launcherLocker.lockProcess(schedule.pipeliteSchedule.getProcessName(), processId)) {
              return;
            }
            processLauncher.run();
            processCompletedCount.incrementAndGet();
          } catch (Exception ex) {
            processFailedCount.incrementAndGet();
            logContext(log.atSevere(), processName, processId)
                .withCause(ex)
                .log("Failed to execute process");
          } finally {
            schedule.getPipeliteSchedule().endExecution();
            pipeliteScheduleService.saveProcessSchedule(schedule.getPipeliteSchedule());
            launcherLocker.unlockProcess(schedule.getPipeliteSchedule().getProcessName(), processId);
            activeProcesses.remove(processId);
            initProcesses.remove(processId);
            taskCompletedCount.addAndGet(processLauncher.getTaskCompletedCount());
            taskSkippedCount.addAndGet(processLauncher.getTaskSkippedCount());
            taskFailedCount.addAndGet(processLauncher.getTaskFailedCount());
          }
        });
  }

  private boolean validateProcess(Schedule schedule, ProcessInstance processInstance) {
    if (processInstance == null) {
      return false;
    }

    boolean isSuccess = processInstance.validate(ProcessInstance.ValidateMode.WITHOUT_TASKS);

    String processName = schedule.getPipeliteSchedule().getProcessName();

    if (!processName.equals(processInstance.getProcessName())) {
      processInstance
          .logContext(log.atSevere())
          .log("Process name is different from launcher process name: %s", processName);
      isSuccess = false;
    }

    return isSuccess;
  }

  @Override
  protected void shutDown() throws Exception {
    logContext(log.atInfo()).log("Shutting down launcher");

    executorService.shutdown();
    try {
      executorService.awaitTermination(ServerManager.FORCE_STOP_WAIT_SECONDS - 1, TimeUnit.SECONDS);
    } catch (InterruptedException ex) {
      executorService.shutdownNow();
      throw ex;
    } finally {
      launcherLocker.unlockLauncher();

      logContext(log.atInfo()).log("Launcher has been shut down");
    }
  }

  public int getActiveProcessCount() {
    return activeProcesses.size();
  }

  public int getProcessCreateFailedCount() {
    return processCreateFailedCount.get();
  }

  public int getProcessStartFailedCount() {
    return processStartFailedCount.get();
  }

  public int getProcessFailedCount() {
    return processFailedCount.get();
  }

  public int getProcessCompletedCount() {
    return processCompletedCount.get();
  }

  public int getTaskFailedCount() {
    return taskFailedCount.get();
  }

  public int getTaskSkippedCount() {
    return taskSkippedCount.get();
  }

  public int getTaskCompletedCount() {
    return taskCompletedCount.get();
  }

  public Long getMaxIterations() {
    return maxIterations;
  }

  public void setMaxIterations(Long maxIterations) {
    this.maxIterations = maxIterations;
  }

  private FluentLogger.Api logContext(FluentLogger.Api log) {
    return log.with(LogKey.LAUNCHER_NAME, launcherName);
  }

  private FluentLogger.Api logContext(FluentLogger.Api log, String processName, String processId) {
    return log.with(LogKey.LAUNCHER_NAME, launcherName)
        .with(LogKey.PROCESS_NAME, processName)
        .with(LogKey.PROCESS_ID, processId);
  }
}
