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
import java.util.function.Supplier;
import java.util.stream.Collectors;
import lombok.extern.flogger.Flogger;
import org.springframework.util.Assert;
import pipelite.configuration.LauncherConfiguration;
import pipelite.entity.ProcessEntity;
import pipelite.lock.PipeliteLocker;
import pipelite.log.LogKey;
import pipelite.process.Process;
import pipelite.process.ProcessFactory;
import pipelite.service.*;

@Flogger
public class PipeliteLauncher extends ProcessLauncherService {

  private final ProcessService processService;
  private final String pipelineName;
  private final ProcessFactory processFactory;
  private final ProcessCreator processCreator;
  private final Duration processQueueMaxRefreshFrequency;
  private final Duration processQueueMinRefreshFrequency;
  private final int processQueueMaxSize;
  private final int processParallelism;
  private final boolean shutdownIfIdle;
  private final ProcessLauncherStats stats = new ProcessLauncherStats();
  protected final List<ProcessEntity> processQueue =
      Collections.synchronizedList(new ArrayList<>());
  private int processQueueIndex = 0;
  private LocalDateTime processQueueMaxValidUntil = LocalDateTime.now();
  private LocalDateTime processQueueMinValidUntil = LocalDateTime.now();

  public PipeliteLauncher(
      LauncherConfiguration launcherConfiguration,
      PipeliteLocker pipeliteLocker,
      ProcessFactory processFactory,
      ProcessService processService,
      ProcessCreator processCreator,
      Supplier<ProcessLauncherPool> processLauncherPoolSupplier,
      String pipelineName) {
    super(
        launcherConfiguration,
        pipeliteLocker,
        launcherName(pipelineName, launcherConfiguration),
        processLauncherPoolSupplier);
    Assert.notNull(launcherConfiguration, "Missing launcher configuration");
    Assert.notNull(processService, "Missing process service");
    Assert.notNull(pipelineName, "Missing pipeline name");
    this.processService = processService;
    this.pipelineName = pipelineName;
    this.processFactory = processFactory;
    this.processCreator = processCreator;
    this.processQueueMaxRefreshFrequency =
        launcherConfiguration.getProcessQueueMaxRefreshFrequency();
    this.processQueueMinRefreshFrequency =
        launcherConfiguration.getProcessQueueMinRefreshFrequency();
    this.processQueueMaxSize = launcherConfiguration.getProcessQueueMaxSize();
    this.processParallelism = launcherConfiguration.getProcessParallelism();
    this.shutdownIfIdle = launcherConfiguration.isShutdownIfIdle();
  }

  private static String launcherName(
      String pipelineName, LauncherConfiguration launcherConfiguration) {
    return LauncherConfiguration.getLauncherName(pipelineName, launcherConfiguration.getPort());
  }

  @Override
  protected void run() {
    if (isQueueProcesses()) {
      processCreator.createProcesses(processQueueMaxSize);
      queueProcesses();
    }
    while (isRunProcess()) {
      runProcess(processQueue.get(processQueueIndex++));
    }
  }

  protected boolean isQueueProcesses() {
    return isQueueProcesses(
        processQueueIndex,
        processQueue.size(),
        processQueueMaxValidUntil,
        processQueueMinValidUntil,
        processParallelism);
  }

  protected boolean isRunProcess() {
    return isRunProcess(
        processQueueIndex, processQueue.size(), activeProcessCount(), processParallelism);
  }

  /** Returns true if the process queue should be recreated. */
  protected static boolean isQueueProcesses(
      int processQueueIndex,
      int processQueueSize,
      LocalDateTime processQueueMaxValidUntil,
      LocalDateTime processQueueMinValidUntil,
      int processParallelism) {
    if (processQueueMinValidUntil.isAfter(LocalDateTime.now())) {
      return false;
    }
    return processQueueIndex >= processQueueSize - processParallelism + 1
        || processQueueMaxValidUntil.isBefore(LocalDateTime.now());
  }

  /** Returns true if a new process should be executed. */
  protected static boolean isRunProcess(
      int processQueueIndex, int processQueueSize, int activeProcessCount, int processParallelism) {
    return processQueueIndex < processQueueSize && activeProcessCount < processParallelism;
  }

  @Override
  protected boolean shutdownIfIdle() {
    return processQueueIndex == processQueue.size() && shutdownIfIdle;
  }

  protected void queueProcesses() {
    logContext(log.atInfo()).log("Queuing process instances");
    // Clear process queue.
    processQueueIndex = 0;
    processQueue.clear();

    // First add active processes. Asynchronous active processes may be able to continue execution.
    processQueue.addAll(getActiveProcesses());

    // Then add new processes.
    processQueue.addAll(getPendingProcesses());
    processQueueMaxValidUntil = LocalDateTime.now().plus(processQueueMaxRefreshFrequency);
    processQueueMinValidUntil = LocalDateTime.now().plus(processQueueMinRefreshFrequency);
  }

  protected List<ProcessEntity> getActiveProcesses() {
    return processService.getActiveProcesses(pipelineName, getLauncherName()).stream()
        .filter(processEntity -> !isProcessActive(pipelineName, processEntity.getProcessId()))
        .collect(Collectors.toList());
  }

  protected List<ProcessEntity> getPendingProcesses() {
    return processService.getPendingProcesses(pipelineName).stream()
        .filter(processEntity -> !isProcessActive(pipelineName, processEntity.getProcessId()))
        .collect(Collectors.toList());
  }

  protected void runProcess(ProcessEntity processEntity) {
    Process process = ProcessFactory.create(processEntity, processFactory, stats);
    if (process != null) {
      run(pipelineName, process, (p, r) -> stats.add(p, r));
    }
  }

  public String getPipelineName() {
    return pipelineName;
  }

  public Duration getProcessQueueMaxRefreshFrequency() {
    return processQueueMaxRefreshFrequency;
  }

  public Duration getProcessQueueMinRefreshFrequency() {
    return processQueueMinRefreshFrequency;
  }

  public LocalDateTime getProcessQueueMaxValidUntil() {
    return processQueueMaxValidUntil;
  }

  public LocalDateTime getProcessQueueMinValidUntil() {
    return processQueueMinValidUntil;
  }

  public ProcessLauncherStats getStats() {
    return stats;
  }

  private FluentLogger.Api logContext(FluentLogger.Api log) {
    return log.with(LogKey.LAUNCHER_NAME, getLauncherName())
        .with(LogKey.PIPELINE_NAME, pipelineName);
  }

  private FluentLogger.Api logContext(FluentLogger.Api log, String processId) {
    return log.with(LogKey.LAUNCHER_NAME, getLauncherName())
        .with(LogKey.PIPELINE_NAME, pipelineName)
        .with(LogKey.PROCESS_ID, processId);
  }
}
