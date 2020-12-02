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
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;
import lombok.extern.flogger.Flogger;
import org.springframework.util.Assert;
import pipelite.configuration.LauncherConfiguration;
import pipelite.configuration.StageConfiguration;
import pipelite.entity.ProcessEntity;
import pipelite.launcher.lock.PipeliteLocker;
import pipelite.log.LogKey;
import pipelite.process.Process;
import pipelite.process.ProcessFactory;
import pipelite.process.ProcessSource;
import pipelite.service.*;

@Flogger
public class PipeliteLauncher extends ProcessLauncherService {

  private static final int DEFAULT_PROCESS_CREATION_COUNT = 5000;
  private final ProcessService processService;
  private final String pipelineName;
  private final ProcessFactory processFactory;
  private final ProcessSource processSource;
  private final Duration processRefreshFrequency;
  private final int pipeliteParallelism;
  private final boolean shutdownIfIdle;
  private final ProcessLauncherStats stats = new ProcessLauncherStats();
  private final List<ProcessEntity> processQueue = Collections.synchronizedList(new ArrayList<>());
  private int processQueueIndex = 0;
  private LocalDateTime processQueueValidUntil = LocalDateTime.now();

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
    super(
        launcherConfiguration,
        new PipeliteLocker(lockService, launcherName(pipelineName, launcherConfiguration)),
        (locker1) ->
            new ProcessLauncherPool(
                locker1,
                (pipelineName1, process1) ->
                    new ProcessLauncher(
                        launcherConfiguration,
                        stageConfiguration,
                        processService,
                        stageService,
                        mailService,
                        pipelineName1,
                        process1)));
    Assert.notNull(launcherConfiguration, "Missing launcher configuration");
    Assert.notNull(stageConfiguration, "Missing stage configuration");
    Assert.notNull(processFactoryService, "Missing process factory service");
    Assert.notNull(processSourceService, "Missing process source service");
    Assert.notNull(processService, "Missing process service");
    Assert.notNull(stageService, "Missing stage service");
    Assert.notNull(mailService, "Missing mail service");
    Assert.notNull(lockService, "Missing lock service");
    Assert.notNull(pipelineName, "Missing pipeline name");
    this.processService = processService;
    this.pipelineName = pipelineName;
    this.processFactory = processFactoryService.create(this.pipelineName);
    this.processSource = processSourceService.create(this.pipelineName);
    this.processRefreshFrequency = launcherConfiguration.getProcessRefreshFrequency();
    this.pipeliteParallelism = launcherConfiguration.getPipelineParallelism();
    this.shutdownIfIdle = launcherConfiguration.isShutdownIfIdle();
  }

  private static String launcherName(
      String pipelineName, LauncherConfiguration launcherConfiguration) {
    return LauncherConfiguration.getLauncherName(pipelineName, launcherConfiguration.getPort());
  }

  @Override
  protected void run() {
    if (processQueueIndex >= processQueue.size()
        || processQueueValidUntil.isBefore(LocalDateTime.now())) {
      if (processSource != null) {
        createNewProcesses();
      }
      queueProcesses();
    }
    for (;
        processQueueIndex < processQueue.size() && getPool().size() < pipeliteParallelism;
        processQueueIndex++) {
      runProcess(processQueue.get(processQueueIndex));
    }
  }

  @Override
  protected boolean shutdownIfIdle() {
    return processQueueIndex == processQueue.size() && shutdownIfIdle;
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
                processEntity ->
                    !getPool().hasActiveProcess(pipelineName, processEntity.getProcessId()))
            .collect(Collectors.toList()));

    // Then add new processes.
    processQueue.addAll(
        processService.getNewProcesses(pipelineName).stream()
            .filter(
                processEntity ->
                    !getPool().hasActiveProcess(pipelineName, processEntity.getProcessId()))
            .collect(Collectors.toList()));
    processQueueValidUntil = LocalDateTime.now().plus(processRefreshFrequency);
  }

  private void runProcess(ProcessEntity processEntity) {
    Process process = ProcessFactory.create(processEntity, processFactory, stats);
    if (process != null) {
      getPool().run(pipelineName, process, (p, r) -> stats.add(p, r));
    }
  }

  public String getPipelineName() {
    return pipelineName;
  }

  public ProcessLauncherStats getStats() {
    return stats;
  }

  private FluentLogger.Api logContext(FluentLogger.Api log) {
    return log.with(LogKey.LAUNCHER_NAME, getLauncherName())
        .with(LogKey.PIPELINE_NAME, pipelineName);
  }
}
