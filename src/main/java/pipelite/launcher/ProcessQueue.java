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
import lombok.extern.flogger.Flogger;
import org.springframework.util.Assert;
import pipelite.configuration.LauncherConfiguration;
import pipelite.entity.ProcessEntity;
import pipelite.log.LogKey;
import pipelite.service.ProcessService;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

@Flogger
public class ProcessQueue {

  private final ProcessService processService;
  private final String launcherName;
  private final String pipelineName;
  private final Duration processQueueMaxRefreshFrequency;
  private final Duration processQueueMinRefreshFrequency;
  private final int processQueueMaxSize;
  private final int processParallelism;
  private final List<ProcessEntity> processQueue = Collections.synchronizedList(new ArrayList<>());
  private AtomicInteger processQueueIndex = new AtomicInteger();
  private LocalDateTime processQueueMaxValidUntil = LocalDateTime.now();
  private LocalDateTime processQueueMinValidUntil = LocalDateTime.now();

  public ProcessQueue(
      LauncherConfiguration launcherConfiguration,
      ProcessService processService,
      String launcherName,
      String pipelineName) {
    Assert.notNull(launcherConfiguration, "Missing launcher configuration");
    Assert.notNull(launcherName, "Missing launcher name");
    Assert.notNull(pipelineName, "Missing pipeline name");
    this.processService = processService;
    this.launcherName = launcherName;
    this.pipelineName = pipelineName;
    this.processQueueMaxRefreshFrequency =
        launcherConfiguration.getProcessQueueMaxRefreshFrequency();
    this.processQueueMinRefreshFrequency =
        launcherConfiguration.getProcessQueueMinRefreshFrequency();
    this.processQueueMaxSize = launcherConfiguration.getProcessQueueMaxSize();
    this.processParallelism = launcherConfiguration.getProcessParallelism();
  }

  /**
   * Returns true of more processes can be queued.
   *
   * @return true if more processes can be queued
   */
  public boolean isFillQueue() {
    return isFillQueue(
        processQueueIndex.get(),
        processQueue.size(),
        processQueueMaxValidUntil,
        processQueueMinValidUntil,
        processParallelism);
  }

  /** Returns true if the process queue should be recreated. */
  public static boolean isFillQueue(
      int processQueueIndex,
      int processQueueSize,
      LocalDateTime processQueueMaxValidUntil,
      LocalDateTime processQueueMinValidUntil,
      int processParallelism) {
    if (processQueueMinValidUntil.isAfter(LocalDateTime.now())) {
      return false;
    }
    return processQueueIndex >= processQueueSize - processParallelism + 1
        || !processQueueMaxValidUntil.isAfter(LocalDateTime.now());
  }

  /**
   * Queues processes and returns the number of processes queued.
   *
   * @return the number of processes queued
   */
  public int fillQueue() {
    if (!isFillQueue()) {
      return 0;
    }
    int processCnt = 0;
    logContext(log.atInfo()).log("Queuing processes");
    // Clear process queue.
    processQueueIndex.set(0);
    processQueue.clear();

    // First add active processes. Asynchronous active processes may be able to continue execution.
    List<ProcessEntity> activeProcesses = getActiveProcesses();
    processCnt += activeProcesses.size();
    processQueue.addAll(activeProcesses);

    // Then add new processes.
    List<ProcessEntity> pendingProcesses = getPendingProcesses();
    processCnt += pendingProcesses.size();
    processQueue.addAll(pendingProcesses);
    processQueueMaxValidUntil = LocalDateTime.now().plus(processQueueMaxRefreshFrequency);
    processQueueMinValidUntil = LocalDateTime.now().plus(processQueueMinRefreshFrequency);
    return processCnt;
  }

  /**
   * Returns true if there are available processes in the queue.
   *
   * @return true if there are available processes in the queue
   */
  public boolean isAvailableProcesses(int activeProcesses) {
    return processQueueIndex.get() < processQueue.size() && activeProcesses < processParallelism;
  }

  /**
   * Returns the number of available processes.
   *
   * @return the number of available processes
   */
  public int countAvailableProcesses(int activeProcesses) {
    return Math.min(
        processQueue.size() - processQueueIndex.get(), processParallelism - activeProcesses);
  }

  /**
   * Returns the next available process in the queue.
   *
   * @return the next available process in the queue
   */
  public ProcessEntity nextAvailableProcess() {
    return processQueue.get(processQueueIndex.getAndIncrement());
  }

  public String getLauncherName() {
    return launcherName;
  }

  public String getPipelineName() {
    return pipelineName;
  }

  protected List<ProcessEntity> getActiveProcesses() {
    return processService.getActiveProcesses(pipelineName, launcherName, processQueueMaxSize);
  }

  protected List<ProcessEntity> getPendingProcesses() {
    return processService.getPendingProcesses(
        pipelineName, processQueueMaxSize - processQueue.size());
  }

  public LocalDateTime getProcessQueueMaxValidUntil() {
    return processQueueMaxValidUntil;
  }

  public LocalDateTime getProcessQueueMinValidUntil() {
    return processQueueMinValidUntil;
  }

  private FluentLogger.Api logContext(FluentLogger.Api log) {
    return log.with(LogKey.LAUNCHER_NAME, launcherName).with(LogKey.PIPELINE_NAME, pipelineName);
  }
}
