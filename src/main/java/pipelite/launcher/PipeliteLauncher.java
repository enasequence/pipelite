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

import static pipelite.configuration.LauncherConfiguration.DEFAULT_PROCESS_LAUNCH_FREQUENCY;

import com.google.common.flogger.FluentLogger;
import com.google.common.util.concurrent.AbstractScheduledService;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;
import lombok.extern.flogger.Flogger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;
import pipelite.configuration.LauncherConfiguration;
import pipelite.configuration.StageConfiguration;
import pipelite.entity.ProcessEntity;
import pipelite.launcher.locker.LauncherLocker;
import pipelite.launcher.locker.ProcessLocker;
import pipelite.log.LogKey;
import pipelite.process.Process;
import pipelite.process.ProcessFactory;
import pipelite.process.ProcessSource;
import pipelite.process.ProcessState;
import pipelite.service.*;

@Flogger
@Component
@Scope("prototype")
public class PipeliteLauncher extends AbstractScheduledService {

  private final LauncherConfiguration launcherConfiguration;
  private final StageConfiguration stageConfiguration;
  private final ProcessFactoryService processFactoryService;
  private final ProcessSourceService processSourceService;
  private final ProcessService processService;
  private final StageService stageService;

  private final String pipelineName;
  private final String launcherName;

  private final LauncherLocker launcherLocker;
  private final ProcessLocker processLocker;

  private final ProcessFactory processFactory;
  private ProcessSource processSource;

  private final int workers;
  private final ExecutorService executorService;

  private final PipeliteLauncherStats stats = new PipeliteLauncherStats();

  private final Map<String, ProcessLauncher> initProcesses = new ConcurrentHashMap<>();
  private final Map<String, ProcessLauncher> activeProcesses = new ConcurrentHashMap<>();

  private final List<ProcessEntity> processQueue = Collections.synchronizedList(new ArrayList<>());
  private int processQueueIndex = 0;
  private LocalDateTime processQueueValidUntil = LocalDateTime.now();

  private boolean shutdownIfIdleTriggered;

  private final Duration processLaunchFrequency;
  private final Duration processRefreshFrequency;

  public PipeliteLauncher(
      @Autowired LauncherConfiguration launcherConfiguration,
      @Autowired StageConfiguration stageConfiguration,
      @Autowired ProcessFactoryService processFactoryService,
      @Autowired ProcessSourceService processSourceService,
      @Autowired ProcessService processService,
      @Autowired StageService stageService,
      @Autowired LockService lockService) {

    this.launcherConfiguration = launcherConfiguration;
    this.stageConfiguration = stageConfiguration;
    this.processFactoryService = processFactoryService;
    this.processSourceService = processSourceService;
    this.processService = processService;
    this.stageService = stageService;

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

    this.pipelineName = launcherConfiguration.getPipelineName();
    if (this.pipelineName == null || this.pipelineName.trim().isEmpty()) {
      throw new IllegalArgumentException("Missing pipeline name from launcher configuration");
    }

    this.launcherName =
        LauncherConfiguration.getLauncherNameForPipeliteLauncher(
            launcherConfiguration, pipelineName);

    this.launcherLocker = new LauncherLocker(this.launcherName, lockService);
    this.processLocker = new ProcessLocker(this.launcherName, lockService);

    this.processFactory = processFactoryService.create(this.pipelineName);
    this.processSource = processSourceService.create(this.pipelineName);

    this.workers =
        launcherConfiguration.getPipelineParallelism() > 0
            ? launcherConfiguration.getPipelineParallelism()
            : LauncherConfiguration.DEFAULT_PROCESS_LAUNCH_PARALLELISM;
    this.executorService = Executors.newFixedThreadPool(workers);
  }

  @Override
  public String serviceName() {
    return launcherName + "/" + pipelineName;
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
    if (processQueueIndex == processQueue.size() && launcherConfiguration.isShutdownIfIdle()) {
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
          ProcessEntity.createExecution(
              pipelineName, newProcess.getProcessId().trim(), newProcess.getPriority());

      processService.saveProcess(newProcessEntity);

      processSource.accept(newProcess.getProcessId());
    }
  }

  private boolean validateNewProcess(ProcessSource.NewProcess newProcess) {
    String processId = newProcess.getProcessId();
    if (processId == null || processId.trim().isEmpty()) {
      logContext(log.atWarning()).log("New process is missing process id");
      stats.processIdMissingCount.incrementAndGet();
      return false;
    }

    processId = processId.trim();

    Optional<ProcessEntity> savedProcessEntity =
        processService.getSavedProcess(pipelineName, processId);

    if (savedProcessEntity.isPresent()) {
      logContext(log.atSevere(), processId)
          .log("New process has non-unique process id %s", processId);
      stats.processIdNotUniqueCount.incrementAndGet();
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
        processService.getActiveProcesses(pipelineName).stream()
            .filter(processEntity -> !activeProcesses.containsKey(processEntity.getProcessId()))
            .collect(Collectors.toList()));

    // Then add new processes.
    processQueue.addAll(
        processService.getNewProcesses(pipelineName).stream()
            .filter(processEntity -> !activeProcesses.containsKey(processEntity.getProcessId()))
            .collect(Collectors.toList()));
    processQueueValidUntil = LocalDateTime.now().plus(processRefreshFrequency);
  }

  private void launchProcess() {
    ProcessEntity processEntity = processQueue.get(processQueueIndex++);
    String processId = processEntity.getProcessId();

    logContext(log.atInfo(), processId).log("Launching process instances");

    Process process = processFactory.create(processId);

    if (process == null) {
      logContext(log.atSevere(), processId).log("Failed to create process: %s", processId);
      stats.processCreationFailedCount.incrementAndGet();
      return;
    }

    ProcessLauncher processLauncher =
        new ProcessLauncher(
            launcherConfiguration,
            stageConfiguration,
            processService,
            stageService,
            pipelineName,
            process,
            processEntity);

    initProcesses.put(processId, processLauncher);

    executorService.execute(
        () -> {
          activeProcesses.put(processId, processLauncher);
          try {
            if (!processLocker.lock(pipelineName, processId)) {
              return;
            }
            ProcessState state = processLauncher.run();
            stats.setProcessExecutionCount(state).incrementAndGet();
          } catch (Exception ex) {
            stats.processExceptionCount.incrementAndGet();
            logContext(log.atSevere(), processId)
                .withCause(ex)
                .log("Failed to execute process because an exception was thrown");
          } finally {
            processLocker.unlock(pipelineName, processId);
            activeProcesses.remove(processId);
            initProcesses.remove(processId);
            stats.stageSuccessCount.addAndGet(processLauncher.getStageSuccessCount());
            stats.stageFailedCount.addAndGet(processLauncher.getStageFailedCount());
          }
        });
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

  public int getActiveProcessCount() {
    return activeProcesses.size();
  }

  public PipeliteLauncherStats getStats() {
    return stats;
  }

  private FluentLogger.Api logContext(FluentLogger.Api log) {
    return log.with(LogKey.LAUNCHER_NAME, launcherName).with(LogKey.PIPELINE_NAME, pipelineName);
  }

  private FluentLogger.Api logContext(FluentLogger.Api log, String processId) {
    return log.with(LogKey.LAUNCHER_NAME, launcherName)
        .with(LogKey.PIPELINE_NAME, pipelineName)
        .with(LogKey.PROCESS_ID, processId);
  }
}
