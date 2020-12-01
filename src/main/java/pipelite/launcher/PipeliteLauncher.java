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
  private final boolean shutdownIfIdle;
  private final ProcessLauncherStats stats = new ProcessLauncherStats();
  private final List<ProcessEntity> processQueue = Collections.synchronizedList(new ArrayList<>());

  private LauncherLockEntity lock;
  private ProcessLauncherPool pool;
  private int processQueueIndex = 0;
  private LocalDateTime processQueueValidUntil = LocalDateTime.now();
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
    this.processLaunchFrequency = launcherConfiguration.getProcessLaunchFrequency();
    this.processRefreshFrequency = launcherConfiguration.getProcessRefreshFrequency();
    this.pipeliteParallelism = launcherConfiguration.getPipelineParallelism();
    this.shutdownIfIdle = launcherConfiguration.isShutdownIfIdle();
  }

  @Override
  protected void startUp() {
    logContext(log.atInfo()).log("Starting up launcher: %s", launcherName);
    lock = lockService.lockLauncher(launcherName);
    if (lock == null) {
      throw new RuntimeException(
          "Could not start launcher " + launcherName + ": could not create lock");
    }
    this.pool =
        new ProcessLauncherPool(
            lockService,
            (pipelineName, process) ->
                new ProcessLauncher(
                    launcherConfiguration,
                    stageConfiguration,
                    processService,
                    stageService,
                    mailService,
                    pipelineName,
                    process),
            lock);
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
    if (!lockService.relockLauncher(lock)) {
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
        processQueueIndex < processQueue.size() && pool.size() < pipeliteParallelism;
        processQueueIndex++) {
      ProcessEntity processEntity = processQueue.get(processQueueIndex);
      runProcess(processEntity);
    }
    shutdownIfIdle();
  }

  private void shutdownIfIdle() throws InterruptedException {
    if (processQueueIndex == processQueue.size() && shutdownIfIdle) {
      logContext(log.atInfo()).log("Stopping idle launcher " + launcherName);
      shutdownIfIdleTriggered = true;
      while (pool.size() > 0) {
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
            .filter(
                processEntity -> !pool.hasActiveProcess(pipelineName, processEntity.getProcessId()))
            .collect(Collectors.toList()));

    // Then add new processes.
    processQueue.addAll(
        processService.getNewProcesses(pipelineName).stream()
            .filter(
                processEntity -> !pool.hasActiveProcess(pipelineName, processEntity.getProcessId()))
            .collect(Collectors.toList()));
    processQueueValidUntil = LocalDateTime.now().plus(processRefreshFrequency);
  }

  private void runProcess(ProcessEntity processEntity) {
    Process process = ProcessFactory.create(processEntity, processFactory, stats);
    if (process != null) {
      pool.run(pipelineName, process, (p, r) -> stats.add(p, r));
    }
  }

  @Override
  protected void shutDown() throws InterruptedException {
    logContext(log.atInfo()).log("Shutting down launcher");
    pool.shutDown();
    logContext(log.atInfo()).log("Launcher has been shut down");
  }

  public String getLauncherName() {
    return launcherName;
  }

  public String getPipelineName() {
    return pipelineName;
  }

  public List<ProcessLauncher> getProcessLaunchers() {
    return pool.get();
  }

  public ProcessLauncherStats getStats() {
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
