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
import org.springframework.util.Assert;
import pipelite.configuration.LauncherConfiguration;
import pipelite.configuration.StageConfiguration;
import pipelite.entity.LauncherLockEntity;
import pipelite.entity.ProcessEntity;
import pipelite.log.LogKey;
import pipelite.process.Process;
import pipelite.process.ProcessFactory;
import pipelite.process.ProcessSource;
import pipelite.process.ProcessState;
import pipelite.service.*;

@Flogger
public class PipeliteLauncher extends AbstractScheduledService {

  private static final int DEFAULT_PROCESS_CREATION_COUNT = 5000;
  private final LauncherConfiguration launcherConfiguration;
  private final StageConfiguration stageConfiguration;
  private final ProcessService processService;
  private final StageService stageService;
  private final LockService lockService;
  private final MailService mailService;
  private final String pipelineName;
  private final String launcherName;
  private final ProcessFactory processFactory;
  private final ProcessSource processSource;
  private final Duration processLaunchFrequency;
  private final Duration processRefreshFrequency;
  private final int pipeliteParallelism;
  private final ExecutorService executorService;
  private final Map<String, ProcessLauncher> activeProcesses = new ConcurrentHashMap<>();
  private final PipeliteLauncherStats stats = new PipeliteLauncherStats();
  private final List<ProcessEntity> processQueue = Collections.synchronizedList(new ArrayList<>());
  private int processQueueIndex = 0;
  private LocalDateTime processQueueValidUntil = LocalDateTime.now();
  private LauncherLockEntity launcherLock;
  private boolean shutdownIfIdleTriggered;

  public PipeliteLauncher(
      LauncherConfiguration launcherConfiguration,
      StageConfiguration stageConfiguration,
      ProcessFactoryService processFactoryService,
      ProcessSourceService processSourceService,
      ProcessService processService,
      StageService stageService,
      LockService lockService,
      MailService mailService,
      String pipelineName) {
    Assert.notNull(launcherConfiguration, "Missing launcher configuration");
    Assert.notNull(stageConfiguration, "Missing stage configuration");
    Assert.notNull(processService, "Missing process service");
    Assert.notNull(processFactoryService, "Missing process factory service");
    Assert.notNull(processSourceService, "Missing process source service");
    Assert.notNull(stageService, "Missing stage service");
    Assert.notNull(mailService, "Missing mail service");
    Assert.notNull(lockService, "Missing lock service");
    Assert.notNull(pipelineName, "Missing pipeline name");
    this.launcherConfiguration = launcherConfiguration;
    this.stageConfiguration = stageConfiguration;
    this.processService = processService;
    this.stageService = stageService;
    this.lockService = lockService;
    this.mailService = mailService;
    this.pipelineName = pipelineName;
    this.launcherName =
        LauncherConfiguration.getLauncherName(pipelineName, launcherConfiguration.getPort());
    this.processFactory = processFactoryService.create(this.pipelineName);
    this.processSource = processSourceService.create(this.pipelineName);

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

    this.pipeliteParallelism =
        launcherConfiguration.getPipelineParallelism() > 0
            ? launcherConfiguration.getPipelineParallelism()
            : LauncherConfiguration.DEFAULT_PIPELINE_PARALLELISM;
    this.executorService = Executors.newFixedThreadPool(pipeliteParallelism);
  }

  @Override
  protected void startUp() {
    logContext(log.atInfo()).log("Starting up launcher: %s", launcherName);
    launcherLock = lockService.lockLauncher(launcherName);
    if (launcherLock == null) {
      throw new RuntimeException(
          "Could not start launcher " + launcherName + ": could not create lock");
    }
  }

  @Override
  public String serviceName() {
    return launcherName;
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

    // Renew launcher lock to avoid lock expiry.
    if (!lockService.relockLauncher(launcherLock)) {
      throw new RuntimeException(
          "Could not continue running launcher " + launcherName + ": could not renew lock");
    }

    if (processQueueIndex >= processQueue.size()
        || processQueueValidUntil.isBefore(LocalDateTime.now())) {
      if (processSource != null) {
        createNewProcesses();
      }
      queueProcesses();
    }
    for (;
        processQueueIndex < processQueue.size() && activeProcesses.size() < pipeliteParallelism;
        processQueueIndex++) {
      ProcessEntity processEntity = processQueue.get(processQueueIndex);
      launchProcess(processEntity);
    }
    shutdownIfIdle();
  }

  private void shutdownIfIdle() throws InterruptedException {
    if (processQueueIndex == processQueue.size() && launcherConfiguration.isShutdownIfIdle()) {
      logContext(log.atInfo()).log("Stopping idle launcher " + launcherName);
      shutdownIfIdleTriggered = true;
      while (!activeProcesses.isEmpty()) {
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
    logContext(log.atInfo()).log("Creating new processes");
    for (int i = 0; i < DEFAULT_PROCESS_CREATION_COUNT; ++i) {
      ProcessSource.NewProcess newProcess = processSource.next();
      if (newProcess == null) {
        // No new processes.
        return;
      }
      String newProcessId = newProcess.getProcessId();
      if (newProcessId == null || newProcessId.trim().isEmpty()) {
        logContext(log.atWarning()).log("New process has no process id");
        processSource.reject(newProcess.getProcessId(), "New process has no process id");
        continue;
      }
      String trimmedNewProcessId = newProcessId.trim();
      Optional<ProcessEntity> savedProcessEntity =
          processService.getSavedProcess(pipelineName, trimmedNewProcessId);
      if (savedProcessEntity.isPresent()) {
        logContext(log.atWarning()).log("New process id already exists %s", trimmedNewProcessId);
        processSource.reject(
            newProcess.getProcessId(), "New process id already exists " + trimmedNewProcessId);
        continue;
      }
      logContext(log.atInfo()).log("Creating new process %s", trimmedNewProcessId);
      processService.createExecution(pipelineName, trimmedNewProcessId, newProcess.getPriority());
      processSource.accept(newProcess.getProcessId());
    }
  }

  private void queueProcesses() {
    logContext(log.atInfo()).log("Queuing process instances");
    // Clear process queue.
    processQueueIndex = 0;
    processQueue.clear();

    // First add active processes. Asynchronous active processes may be able to continue execution.
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

  private void launchProcess(ProcessEntity processEntity) {
    String processId = processEntity.getProcessId();
    logContext(log.atInfo(), processId).log("Preparing to launch process: %s", processId);
    // Create process.
    Process process = processFactory.create(processId);
    if (process == null) {
      logContext(log.atSevere(), processId).log("Failed to create process: %s", processId);
      stats.processCreationFailedCount.incrementAndGet();
      return;
    }
    // Lock process.
    if (!lockService.lockProcess(launcherLock, pipelineName, processId)) {
      return;
    }
    process.setProcessEntity(processEntity);
    // Create process launcher.
    ProcessLauncher processLauncher =
        new ProcessLauncher(
            launcherConfiguration,
            stageConfiguration,
            processService,
            stageService,
            mailService,
            pipelineName,
            process);
    activeProcesses.put(processId, processLauncher);
    // Run process.
    executorService.execute(
        () -> {
          try {
            ProcessState state = processLauncher.run();
            stats.setProcessExecutionCount(state).incrementAndGet();
          } catch (Exception ex) {
            stats.processExceptionCount.incrementAndGet();
            logContext(log.atSevere(), processId)
                .withCause(ex)
                .log("Failed to execute process because an exception was thrown");
          } finally {
            lockService.unlockProcess(launcherLock, pipelineName, processId);
            activeProcesses.remove(processId);
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
      lockService.unlockProcesses(launcherLock);
      lockService.unlockLauncher(launcherLock);
      logContext(log.atInfo()).log("Launcher has been shut down");
    }
  }

  public String getLauncherName() {
    return launcherName;
  }

  public String getPipelineName() {
    return pipelineName;
  }

  public Map<String, ProcessLauncher> getActiveProcesses() {
    return activeProcesses;
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
