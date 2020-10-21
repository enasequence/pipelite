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
import pipelite.configuration.StageConfiguration;
import pipelite.entity.ProcessEntity;
import pipelite.launcher.locker.LauncherLocker;
import pipelite.launcher.locker.ProcessLocker;
import pipelite.log.LogKey;
import pipelite.process.Process;
import pipelite.process.ProcessFactory;
import pipelite.process.ProcessSource;
import pipelite.service.*;

import static pipelite.configuration.LauncherConfiguration.DEFAULT_PROCESS_LAUNCH_FREQUENCY;

@Flogger
@Component
@Scope("prototype")
public class PipeliteLauncher extends AbstractScheduledService {

  private final LauncherConfiguration launcherConfiguration;
  private final ProcessConfiguration processConfiguration;
  private final StageConfiguration stageConfiguration;
  private final ProcessFactoryService processFactoryService;
  private final ProcessSourceService processSourceService;
  private final ProcessService processService;
  private final StageService stageService;
  private final String launcherName;
  private final LauncherLocker launcherLocker;
  private final ProcessLocker processLocker;
  private final ExecutorService executorService;
  private final int workers;
  private final ProcessFactory processFactory;
  private ProcessSource processSource;

  private final AtomicInteger processFailedToCreateCount = new AtomicInteger(0);
  private final AtomicInteger processExceptionCount = new AtomicInteger(0);
  private final AtomicInteger processCompletedCount = new AtomicInteger(0);
  private final AtomicInteger stageFailedCount = new AtomicInteger(0);
  private final AtomicInteger stageCompletedCount = new AtomicInteger(0);

  private final Map<String, ProcessLauncher> initProcesses = new ConcurrentHashMap<>();
  private final Map<String, ProcessLauncher> activeProcesses = new ConcurrentHashMap<>();

  private final ArrayList<ProcessEntity> processQueue = new ArrayList<>();
  private int processQueueIndex = 0;
  private LocalDateTime processQueueValidUntil = LocalDateTime.now();

  private boolean shutdownIfIdle;
  private boolean shutdownIfIdleTriggered;

  private final Duration processLaunchFrequency;
  private final Duration processRefreshFrequency;

  public PipeliteLauncher(
      @Autowired LauncherConfiguration launcherConfiguration,
      @Autowired ProcessConfiguration processConfiguration,
      @Autowired StageConfiguration stageConfiguration,
      @Autowired ProcessFactoryService processFactoryService,
      @Autowired ProcessSourceService processSourceService,
      @Autowired ProcessService processService,
      @Autowired StageService stageService,
      @Autowired LockService lockService) {

    if (processConfiguration.getPipelineName() == null
        || processConfiguration.getPipelineName().trim().isEmpty()) {
      throw new IllegalArgumentException("Missing pipeline name");
    }
    this.launcherConfiguration = launcherConfiguration;
    this.processConfiguration = processConfiguration;
    this.stageConfiguration = stageConfiguration;
    this.processFactoryService = processFactoryService;
    this.processSourceService = processSourceService;
    this.processService = processService;
    this.stageService = stageService;
    this.workers =
        launcherConfiguration.getProcessLaunchParallelism() > 0
            ? launcherConfiguration.getProcessLaunchParallelism()
            : LauncherConfiguration.DEFAULT_PROCESS_LAUNCH_PARALLELISM;
    this.executorService = Executors.newFixedThreadPool(workers);

    if (launcherConfiguration.getProcessLaunchFrequency() != null) {
      this.processLaunchFrequency = launcherConfiguration.getProcessLaunchFrequency();
    } else {
      this.processLaunchFrequency = DEFAULT_PROCESS_LAUNCH_FREQUENCY;
    }

    if (launcherConfiguration.getProcessRefreshFrequency() != null) {
      this.processRefreshFrequency = launcherConfiguration.getProcessRefreshFrequency();
    } else {
      this.processRefreshFrequency = LauncherConfiguration.DEFAULT_PROCESS_REFRESH_FREQUENCY;
    }

    this.processFactory = processFactoryService.create(getPipelineName());
    this.processSource = processSourceService.create(getPipelineName());

    this.launcherName =
        LauncherConfiguration.getLauncherNameForPipeliteLauncher(
            launcherConfiguration, processFactory.getPipelineName());
    this.launcherLocker = new LauncherLocker(this.launcherName, lockService);
    this.processLocker = new ProcessLocker(this.launcherName, lockService);
  }

  @Override
  public String serviceName() {
    return launcherName + "/" + getPipelineName();
  }

  @Override
  protected void startUp() {
    logContext(log.atInfo()).log("Starting up launcher");

    if (!launcherLocker.lock()) {
      throw new RuntimeException("Could not start launcher");
    }
  }

  @Override
  protected Scheduler scheduler() {
    return Scheduler.newFixedDelaySchedule(Duration.ZERO, processLaunchFrequency);
  }

  @Override
  protected void runOneIteration() throws Exception {
    if (shutdownIfIdleTriggered || !isRunning()) {
      return;
    }

    logContext(log.atInfo()).log("Running launcher");

    if (processQueueIndex >= processQueue.size()
        || processQueueValidUntil.isBefore(LocalDateTime.now())) {
      if (processSource != null) {
        createNewProcesses();
      }
      queueProcesses();
    }

    while (processQueueIndex < processQueue.size() && initProcesses.size() < workers) {
      launchProcess();
    }

    shutdownIfIdle();
  }

  private void shutdownIfIdle() throws InterruptedException {
    if (processQueueIndex == processQueue.size() && shutdownIfIdle) {
      logContext(log.atInfo()).log("Stopping idle pipelite launcher");
      shutdownIfIdleTriggered = true;
      while (!initProcesses.isEmpty()) {
        try {
          Thread.sleep(Duration.ofSeconds(1).toMillis());
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

      ProcessEntity newProcessEntity =
          ProcessEntity.newExecution(
              newProcess.getProcessId().trim(), getPipelineName(), newProcess.getPriority());

      processService.saveProcess(newProcessEntity);

      processSource.accept(newProcess.getProcessId());
    }
  }

  private boolean validateNewProcess(ProcessSource.NewProcess newProcess) {
    String processId = newProcess.getProcessId();
    if (processId == null || processId.trim().isEmpty()) {
      logContext(log.atWarning()).log("Could not create process instance without a process id");
      processFailedToCreateCount.incrementAndGet();
      return false;
    }

    processId = processId.trim();

    Optional<ProcessEntity> savedProcessEntity =
        processService.getSavedProcess(getPipelineName(), processId);

    if (savedProcessEntity.isPresent()) {
      logContext(log.atSevere(), processId)
          .log("Could not create process instance with a process id that already exists");
      processFailedToCreateCount.incrementAndGet();
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
        processService.getActiveProcesses(getPipelineName()).stream()
            .filter(processEntity -> !activeProcesses.containsKey(processEntity.getProcessId()))
            .collect(Collectors.toList()));

    // Then add new processes.
    processQueue.addAll(
        processService.getNewProcesses(getPipelineName()).stream()
            .filter(processEntity -> !activeProcesses.containsKey(processEntity.getProcessId()))
            .collect(Collectors.toList()));
    processQueueValidUntil = LocalDateTime.now().plus(processRefreshFrequency);
  }

  private void launchProcess() {
    ProcessEntity processEntity = processQueue.get(processQueueIndex++);
    String processId = processEntity.getProcessId();

    logContext(log.atInfo(), processId).log("Launching process instances");

    ProcessLauncher processLauncher =
        new ProcessLauncher(
            launcherConfiguration, stageConfiguration, processService, stageService);

    Process process = processFactory.create(processId);

    if (process == null) {
      logContext(log.atSevere(), processId).log("Could not create process instance");
      processFailedToCreateCount.incrementAndGet();
      return;
    }

    if (!validateProcess(process)) {
      logContext(log.atSevere(), processId).log("Failed to validate process instance");
      processFailedToCreateCount.incrementAndGet();
      return;
    }

    processLauncher.init(process, processEntity);
    initProcesses.put(processId, processLauncher);

    executorService.execute(
        () -> {
          activeProcesses.put(processId, processLauncher);
          try {
            if (!processLocker.lock(getPipelineName(), processId)) {
              return;
            }
            processLauncher.run();
            processCompletedCount.incrementAndGet();
          } catch (Exception ex) {
            processExceptionCount.incrementAndGet();
            logContext(log.atSevere(), processId).withCause(ex).log("Failed to execute process");
          } finally {
            processLocker.unlock(getPipelineName(), processId);
            activeProcesses.remove(processId);
            initProcesses.remove(processId);
            stageCompletedCount.addAndGet(processLauncher.getStageCompletedCount());
            stageFailedCount.addAndGet(processLauncher.getStageFailedCount());
          }
        });
  }

  private boolean validateProcess(Process process) {
    if (process == null) {
      return false;
    }

    boolean isSuccess = process.validate(Process.ValidateMode.WITHOUT_STAGES);

    if (!getPipelineName().equals(process.getPipelineName())) {
      process
          .logContext(log.atSevere())
          .log("Pipeline name is different from launcher pipeline name: %s", getPipelineName());
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
      launcherLocker.unlock();

      logContext(log.atInfo()).log("Launcher has been shut down");
    }
  }

  public void removeLocks() {
    launcherLocker.removeLocks();
  }

  public String getPipelineName() {
    return processConfiguration.getPipelineName();
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

  public boolean isShutdownIfIdle() {
    return shutdownIfIdle;
  }

  public void setShutdownIfIdle(boolean shutdownIfIdle) {
    this.shutdownIfIdle = shutdownIfIdle;
  }

  private FluentLogger.Api logContext(FluentLogger.Api log) {
    return log.with(LogKey.LAUNCHER_NAME, launcherName)
        .with(LogKey.PIPELINE_NAME, getPipelineName());
  }

  private FluentLogger.Api logContext(FluentLogger.Api log, String processId) {
    return log.with(LogKey.LAUNCHER_NAME, launcherName)
        .with(LogKey.PIPELINE_NAME, getPipelineName())
        .with(LogKey.PROCESS_ID, processId);
  }
}
