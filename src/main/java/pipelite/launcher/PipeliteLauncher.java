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
import java.util.stream.Collectors;
import lombok.extern.flogger.Flogger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;
import pipelite.configuration.LauncherConfiguration;
import pipelite.configuration.ProcessConfiguration;
import pipelite.configuration.TaskConfiguration;
import pipelite.entity.PipeliteProcess;
import pipelite.log.LogKey;
import pipelite.process.ProcessFactory;
import pipelite.process.ProcessInstance;
import pipelite.process.ProcessSource;
import pipelite.service.PipeliteLockService;
import pipelite.service.PipeliteProcessService;
import pipelite.service.PipeliteStageService;

@Flogger
@Component
@Scope("prototype")
public class PipeliteLauncher extends AbstractScheduledService {

  private final LauncherConfiguration launcherConfiguration;
  private final ProcessConfiguration processConfiguration;
  private final TaskConfiguration taskConfiguration;
  private final PipeliteProcessService pipeliteProcessService;
  private final PipeliteStageService pipeliteStageService;
  private final PipeliteLockService pipeliteLockService;
  private final ExecutorService executorService;
  private final int workers;
  private ProcessFactory processFactory;
  private ProcessSource processSource;

  private final AtomicInteger processSourceFailedCount = new AtomicInteger(0);
  private final AtomicInteger processFactoryFailedCount = new AtomicInteger(0);
  private final AtomicInteger processStartFailedCount = new AtomicInteger(0);
  private final AtomicInteger processFailedCount = new AtomicInteger(0);
  private final AtomicInteger processCompletedCount = new AtomicInteger(0);
  private final AtomicInteger taskFailedCount = new AtomicInteger(0);
  private final AtomicInteger taskSkippedCount = new AtomicInteger(0);
  private final AtomicInteger taskCompletedCount = new AtomicInteger(0);

  private final Map<String, ProcessLauncher> initProcesses = new ConcurrentHashMap<>();
  private final Map<String, ProcessLauncher> activeProcesses = new ConcurrentHashMap<>();

  private final ArrayList<PipeliteProcess> processQueue = new ArrayList<>();
  private int processQueueIndex = 0;
  private LocalDateTime processQueueValidUntil = LocalDateTime.now();

  private ShutdownPolicy shutdownPolicy = ShutdownPolicy.WAIT_IF_IDLE;
  private long iterations = 0;
  private Long maxIterations;

  public static final int DEFAULT_WORKERS = ForkJoinPool.getCommonPoolParallelism();
  public static final Duration DEFAULT_RUN_DELAY = Duration.ofMinutes(1);
  public static final Duration DEFAULT_STOP_DELAY = Duration.ofSeconds(1);
  public static final Duration DEFAULT_REFRESH_DELAY = Duration.ofHours(1);
  private final Duration runDelay;
  private final Duration stopDelay = DEFAULT_STOP_DELAY;
  private final Duration refreshDelay;

  public PipeliteLauncher(
      @Autowired LauncherConfiguration launcherConfiguration,
      @Autowired ProcessConfiguration processConfiguration,
      @Autowired TaskConfiguration taskConfiguration,
      @Autowired PipeliteProcessService pipeliteProcessService,
      @Autowired PipeliteStageService pipeliteStageService,
      @Autowired PipeliteLockService pipeliteLockService) {
    this.launcherConfiguration = launcherConfiguration;
    this.processConfiguration = processConfiguration;
    this.taskConfiguration = taskConfiguration;
    this.pipeliteProcessService = pipeliteProcessService;
    this.pipeliteStageService = pipeliteStageService;
    this.pipeliteLockService = pipeliteLockService;
    this.workers =
        launcherConfiguration.getWorkers() > 0
            ? launcherConfiguration.getWorkers()
            : DEFAULT_WORKERS;
    this.executorService = Executors.newFixedThreadPool(workers);

    if (launcherConfiguration.getRunDelay() != null) {
      this.runDelay = launcherConfiguration.getRunDelay();
    } else {
      this.runDelay = DEFAULT_RUN_DELAY;
    }

    if (launcherConfiguration.getRefreshDelay() != null) {
      this.refreshDelay = launcherConfiguration.getRefreshDelay();
    } else {
      this.refreshDelay = DEFAULT_REFRESH_DELAY;
    }
  }

  @Override
  public String serviceName() {
    return getLauncherName() + "/" + getProcessName();
  }

  @Override
  protected void startUp() {
    logContext(log.atInfo()).log("Starting up launcher");

    if (!lockLauncher()) {
      throw new RuntimeException("Could not start launcher");
    }

    processFactory = ProcessConfiguration.getProcessFactory(processConfiguration);
    if (processConfiguration.isProcessSource()) {
      processSource = ProcessConfiguration.getProcessSource(processConfiguration);
    }
  }

  @Override
  protected Scheduler scheduler() {
    return Scheduler.newFixedDelaySchedule(Duration.ZERO, runDelay);
  }

  @Override
  protected void runOneIteration() throws Exception {
    if (!isRunning()) {
      return;
    }

    logContext(log.atInfo()).log("Running launcher");

    if (processQueueIndex >= processQueue.size()
        || processQueueValidUntil.isBefore(LocalDateTime.now())) {
      if (processConfiguration.isProcessSource()) {
        createNewProcesses();
      }
      queueProcesses();
    }

    while (processQueueIndex < processQueue.size() && initProcesses.size() < workers) {
      launchProcess();
    }

    stopIfMaxIterations();
    stopIfIdle();
  }

  private void stopIfMaxIterations() {
    if (maxIterations != null && ++iterations > maxIterations) {
      stopAsync();
    }
  }

  private void stopIfIdle() throws InterruptedException {
    if (processQueueIndex == processQueue.size()
        && ShutdownPolicy.SHUTDOWN_IF_IDLE.equals(shutdownPolicy)) {
      logContext(log.atInfo()).log("Shutting down no new active processes to launch");

      while (!initProcesses.isEmpty()) {
        try {
          Thread.sleep(stopDelay.toMillis());
        } catch (InterruptedException ex) {
          throw ex;
        }
      }
      stopAsync();
    }
  }

  private void createNewProcesses() {
    logContext(log.atInfo()).log("Creating new process instances");

    while (true) {
      ProcessSource.NewProcess newProcess = processSource.next();
      if (newProcess == null) {
        break;
      }

      if (!validateNewProcess(newProcess)) {
        continue;
      }

      PipeliteProcess newPipeliteProcess =
          PipeliteProcess.newExecution(
              newProcess.getProcessId().trim(), getProcessName(), newProcess.getPriority());

      pipeliteProcessService.saveProcess(newPipeliteProcess);

      processSource.accept(newProcess.getProcessId());
    }
  }

  private boolean validateNewProcess(ProcessSource.NewProcess newProcess) {
    String processId = newProcess.getProcessId();
    if (processId == null || processId.trim().isEmpty()) {
      logContext(log.atWarning()).log("Ignored new process instance. Process id was missing.");
      processSourceFailedCount.incrementAndGet();
      return false;
    }

    processId = processId.trim();

    Optional<PipeliteProcess> savedPipeliteProcess =
        pipeliteProcessService.getSavedProcess(getProcessName(), processId);

    if (savedPipeliteProcess.isPresent()) {
      logContext(log.atSevere(), processId)
          .log("Rejected new process instance. Process id already exists.");
      processSourceFailedCount.incrementAndGet();
      processSource.reject(processId);
      return false;
    }

    return true;
  }

  private void queueProcesses() {

    logContext(log.atInfo()).log("Queuing process instances");

    // Clear process queue.
    processQueueIndex = 0;
    processQueue.clear();

    // First add active processes in case we can recover them.
    processQueue.addAll(
        pipeliteProcessService.getActiveProcesses(getProcessName()).stream()
            .filter(pipeliteProcess -> !activeProcesses.containsKey(pipeliteProcess.getProcessId()))
            .collect(Collectors.toList()));

    // Then add new processes.
    processQueue.addAll(
        pipeliteProcessService.getNewProcesses(getProcessName()).stream()
            .filter(pipeliteProcess -> !activeProcesses.containsKey(pipeliteProcess.getProcessId()))
            .collect(Collectors.toList()));
    processQueueValidUntil = LocalDateTime.now().plus(refreshDelay);
  }

  private void launchProcess() {
    PipeliteProcess pipeliteProcess = processQueue.get(processQueueIndex++);
    String processId = pipeliteProcess.getProcessId();

    logContext(log.atInfo(), processId).log("Launching process instances");

    ProcessLauncher processLauncher =
        new ProcessLauncher(
            launcherConfiguration,
            taskConfiguration,
            pipeliteProcessService,
            pipeliteStageService);

    ProcessInstance processInstance = processFactory.create(processId);

    if (processInstance == null || !validateProcess(processInstance)) {
      logContext(log.atSevere(), processId).log("Invalid process instance");
      processFactoryFailedCount.incrementAndGet();
      return;
    }

    processLauncher.init(processInstance, pipeliteProcess);
    initProcesses.put(processId, processLauncher);

    executorService.execute(
        () -> {
          activeProcesses.put(processId, processLauncher);
          try {
            if (!lockProcess(processId)) {
              return;
            }
            processLauncher.run();
            processCompletedCount.incrementAndGet();
          } catch (Exception ex) {
            processFailedCount.incrementAndGet();
            logContext(log.atSevere(), processId).withCause(ex).log("Failed to execute process");
          } finally {
            unlockProcess(processId);
            activeProcesses.remove(processId);
            initProcesses.remove(processId);
            taskCompletedCount.addAndGet(processLauncher.getTaskCompletedCount());
            taskSkippedCount.addAndGet(processLauncher.getTaskSkippedCount());
            taskFailedCount.addAndGet(processLauncher.getTaskFailedCount());
          }
        });
  }

  private boolean validateProcess(ProcessInstance processInstance) {
    if (processInstance == null) {
      return false;
    }

    boolean isSuccess = processInstance.validate(ProcessInstance.ValidateMode.WITHOUT_TASKS);

    if (!getProcessName().equals(processInstance.getProcessName())) {
      processInstance
          .logContext(log.atSevere())
          .log("Process name is different from launcher process name: %s", getProcessName());
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
      unlockLauncher();

      logContext(log.atInfo()).log("Launcher has been shut down");
    }
  }

  private boolean lockLauncher() {
    logContext(log.atInfo()).log("Attempting to lock launcher");

    if (pipeliteLockService.lockLauncher(getLauncherName(), getProcessName())) {
      logContext(log.atInfo()).log("Locked launcher");
      return true;
    }
    logContext(log.atWarning()).log("Failed to lock launcher");
    return false;
  }

  private void unlockLauncher() {
    logContext(log.atInfo()).log("Attempting to unlock launcher");

    if (pipeliteLockService.unlockLauncher(getLauncherName(), getProcessName())) {
      logContext(log.atInfo()).log("Unlocked launcher");
    } else {
      logContext(log.atInfo()).log("Failed to unlock launcher");
    }
  }

  private boolean lockProcess(String processId) {
    logContext(log.atInfo(), processId).log("Attempting to lock process launcher");

    if (pipeliteLockService.lockProcess(getLauncherName(), getProcessName(), processId)) {
      logContext(log.atInfo(), processId).log("Locked process launcher");
      return true;
    } else {
      if (pipeliteLockService.isProcessLocked(getLauncherName(), getProcessName(), processId)) {
        logContext(log.atInfo(), processId).log("Process launcher already locked");
        return true;
      }

      logContext(log.atWarning(), processId).log("Failed to lock process launcher");
      return false;
    }
  }

  private void unlockProcess(String processId) {
    logContext(log.atInfo(), processId).log("Attempting to unlock process launcher");
    if (pipeliteLockService.unlockProcess(getLauncherName(), getProcessName(), processId)) {
      logContext(log.atInfo(), processId).log("Unlocked process launcher");
    } else {
      logContext(log.atInfo(), processId).log("Failed to unlock process launcher");
    }
  }

  public String getLauncherName() {
    return launcherConfiguration.getLauncherName();
  }

  public String getProcessName() {
    return processConfiguration.getProcessName();
  }

  public int getActiveProcessCount() {
    return activeProcesses.size();
  }

  public int getProcessSourceFailedCount() {
    return processSourceFailedCount.get();
  }

  public int getProcessFactoryFailedCount() {
    return processFactoryFailedCount.get();
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

  public ShutdownPolicy getShutdownPolicy() {
    return shutdownPolicy;
  }

  public void setShutdownPolicy(ShutdownPolicy shutdownPolicy) {
    this.shutdownPolicy = shutdownPolicy;
  }

  public Long getMaxIterations() {
    return maxIterations;
  }

  public void setMaxIterations(Long maxIterations) {
    this.maxIterations = maxIterations;
  }

  private FluentLogger.Api logContext(FluentLogger.Api log) {
    return log.with(LogKey.LAUNCHER_NAME, getLauncherName())
        .with(LogKey.PROCESS_NAME, getProcessName());
  }

  private FluentLogger.Api logContext(FluentLogger.Api log, String processId) {
    return log.with(LogKey.LAUNCHER_NAME, getLauncherName())
        .with(LogKey.PROCESS_NAME, getProcessName())
        .with(LogKey.PROCESS_ID, processId);
  }
}
