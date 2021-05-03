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
package pipelite.runner.process;

import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.springframework.util.Assert;
import pipelite.configuration.AdvancedConfiguration;
import pipelite.entity.ProcessEntity;
import pipelite.service.ProcessService;

public class ProcessQueue {

  private final ProcessService processService;
  private final String pipelineName;
  private final Duration processQueueMaxRefreshFrequency;
  private final Duration processQueueMinRefreshFrequency;
  private final int processQueueMaxSize;
  private final int pipelineParallelism;
  private final List<ProcessEntity> processQueue = Collections.synchronizedList(new ArrayList<>());
  private final AtomicInteger processQueueIndex = new AtomicInteger();
  private ZonedDateTime processQueueMaxValidUntil = ZonedDateTime.now();
  private ZonedDateTime processQueueMinValidUntil = ZonedDateTime.now();

  public ProcessQueue(
      AdvancedConfiguration advancedConfiguration,
      ProcessService processService,
      String pipelineName,
      int pipelineParallelism) {
    Assert.notNull(advancedConfiguration, "Missing advanced configuration");
    Assert.notNull(pipelineName, "Missing pipeline name");
    this.processService = processService;
    this.pipelineName = pipelineName;
    this.processQueueMaxRefreshFrequency =
        advancedConfiguration.getProcessQueueMaxRefreshFrequency();
    this.processQueueMinRefreshFrequency =
        advancedConfiguration.getProcessQueueMinRefreshFrequency();
    this.processQueueMaxSize = advancedConfiguration.getProcessQueueMaxSize();
    this.pipelineParallelism = Math.max(pipelineParallelism, 1);
  }

  /**
   * Returns the pipeline name.
   *
   * @return the pipeline name
   */
  public String getPipelineName() {
    return pipelineName;
  }

  /**
   * Returns true if more processes can be queued.
   *
   * @return true if more processes can be queued
   */
  public boolean isFillQueue() {
    return isFillQueue(
        processQueueIndex.get(),
        processQueue.size(),
        processQueueMaxValidUntil,
        processQueueMinValidUntil,
        pipelineParallelism);
  }

  /** Returns true if the process queue should be recreated. */
  public static boolean isFillQueue(
      int processQueueIndex,
      int processQueueSize,
      ZonedDateTime processQueueMaxValidUntil,
      ZonedDateTime processQueueMinValidUntil,
      int pipelineParallelism) {
    if (processQueueMinValidUntil.isAfter(ZonedDateTime.now())) {
      return false;
    }
    return processQueueIndex >= processQueueSize - pipelineParallelism + 1
        || !processQueueMaxValidUntil.isAfter(ZonedDateTime.now());
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
    // Clear process queue.
    processQueueIndex.set(0);
    processQueue.clear();

    // First add unlocked active processes. Asynchronous active processes may be able to continue
    // execution.
    List<ProcessEntity> unlockedActiveProcesses = getUnlockedActiveProcesses();
    processCnt += unlockedActiveProcesses.size();
    processQueue.addAll(unlockedActiveProcesses);

    // Then add new processes.
    List<ProcessEntity> pendingProcesses = getPendingProcesses();
    processCnt += pendingProcesses.size();
    processQueue.addAll(pendingProcesses);
    processQueueMaxValidUntil = ZonedDateTime.now().plus(processQueueMaxRefreshFrequency);
    processQueueMinValidUntil = ZonedDateTime.now().plus(processQueueMinRefreshFrequency);
    return processCnt;
  }

  /**
   * Returns true if there are available processes in the queue taking into account the maximum
   * number of active processes.
   *
   * @param activeProcesses the number of currently executing processes
   * @return true if there are available processes in the queue
   */
  public boolean isAvailableProcesses(int activeProcesses) {
    return processQueueIndex.get() < processQueue.size() && activeProcesses < pipelineParallelism;
  }

  /**
   * Returns the number of available processes in the queue.
   *
   * @return the number of available processes in the queue
   */
  public int getQueuedProcessCount() {
    return processQueue.size() - processQueueIndex.get();
  }

  /**
   * Returns the next available process in the queue.
   *
   * @return the next available process in the queue
   */
  public ProcessEntity nextAvailableProcess() {
    return processQueue.get(processQueueIndex.getAndIncrement());
  }

  public List<ProcessEntity> getUnlockedActiveProcesses() {
    return processService.getUnlockedActiveProcesses(pipelineName, processQueueMaxSize);
  }

  public List<ProcessEntity> getPendingProcesses() {
    return processService.getPendingProcesses(
        pipelineName, processQueueMaxSize - processQueue.size());
  }

  public ZonedDateTime getProcessQueueMaxValidUntil() {
    return processQueueMaxValidUntil;
  }

  public ZonedDateTime getProcessQueueMinValidUntil() {
    return processQueueMinValidUntil;
  }
}