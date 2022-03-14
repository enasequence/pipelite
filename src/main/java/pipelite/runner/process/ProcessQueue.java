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
import java.util.*;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import org.springframework.util.Assert;
import pipelite.Pipeline;
import pipelite.configuration.PipeliteConfiguration;
import pipelite.entity.ProcessEntity;
import pipelite.service.PipeliteServices;
import pipelite.service.ProcessService;

public class ProcessQueue {

  public static final int MAX_PARALLELISM = 100000;

  /**
   * The queue is created by selecting processes from the database based on process status and
   * priority. It is fairly expensive to create the queue. Therefore the queue should be created as
   * infrequently as possible. However, because processes are prioritised it is important to refresh
   * the queue periodically to prioritize the execution of new high priority processes. In an
   * attempt to balance these two requirements the process queue length is determined by a
   * parallelism multiplier.
   */
  public static final int MAX_QUEUE_SIZE_PARALLELISM_MULTIPLIER = 4;

  private final ProcessService processService;
  private final String pipelineName;
  private final int pipelineParallelism;
  private final Duration minRefreshFrequency;
  private final Duration maxRefreshFrequency;
  private ZonedDateTime refreshTime;
  private final Queue<ProcessEntity> processQueue = new ArrayDeque<>();
  private final int processQueueMaxSize;
  private int processQueueRefreshSize = 0;

  /**
   * The refreshQueue and nextProcess methods are called from different threads. The refreshLock
   * makes sure that they are not called at the same time. The refreshQueue method will wait until
   * the lock can be acquired. The nextProcess method will return null if the lock can't be
   * acquired.
   */
  private final ReentrantLock refreshLock = new ReentrantLock();

  /**
   * When the refreshQueue method is called some new processes may already have been returned by the
   * nextProcess method call. These processes are removed using the returnedProcesses set.
   */
  private final Set<String> returnedProcesses = new HashSet();

  public ProcessQueue(
      PipeliteConfiguration pipeliteConfiguration,
      PipeliteServices pipeliteServices,
      Pipeline pipeline) {
    Assert.notNull(pipeliteConfiguration, "Missing pipelite configuration");
    Assert.notNull(pipeliteServices, "Missing pipelite services");
    Assert.notNull(pipeline, "Missing pipeline");
    Assert.notNull(pipeline.pipelineName(), "Missing pipeline name");
    Assert.notNull(
        pipeline.configurePipeline().pipelineParallelism(), "Missing pipeline parallelism");

    this.processService = pipeliteServices.process();
    this.pipelineName = pipeline.pipelineName();
    this.pipelineParallelism =
        Math.min(MAX_PARALLELISM, Math.max(pipeline.configurePipeline().pipelineParallelism(), 1));

    this.minRefreshFrequency =
        pipeline.configurePipeline().processQueueMinRefreshFrequency() != null
            ? pipeline.configurePipeline().processQueueMinRefreshFrequency()
            : pipeliteConfiguration.advanced().getProcessQueueMinRefreshFrequency();

    this.maxRefreshFrequency =
        pipeline.configurePipeline().processQueueMaxRefreshFrequency() != null
            ? pipeline.configurePipeline().processQueueMaxRefreshFrequency()
            : pipeliteConfiguration.advanced().getProcessQueueMaxRefreshFrequency();

    Assert.notNull(this.minRefreshFrequency, "Missing process queue min refresh frequency");
    Assert.notNull(this.maxRefreshFrequency, "Missing process queue max refresh frequency");

    this.processQueueMaxSize = pipelineParallelism * MAX_QUEUE_SIZE_PARALLELISM_MULTIPLIER;
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
   * Returns true if the queue should be refreshed.
   *
   * @return true if the queue should be refreshed.
   */
  public boolean isRefreshQueue() {
    if (refreshTime == null) {
      // Queue has not been refreshed.
      return true;
    }
    ZonedDateTime now = ZonedDateTime.now();
    if (isRefreshQueuePremature(now, refreshTime, minRefreshFrequency)) {
      return false;
    }
    if (isRefreshQueueOverdue(now, refreshTime, maxRefreshFrequency)) {
      return true;
    }
    return isRefreshQueueSize();
  }

  public boolean isRefreshQueueSize() {
    return processQueue.size() <= pipelineParallelism;
  }

  public static boolean isRefreshQueuePremature(
      ZonedDateTime now, ZonedDateTime refreshTime, Duration minRefreshFrequency) {
    return now.isBefore(refreshTime.plus(minRefreshFrequency));
  }

  public static boolean isRefreshQueueOverdue(
      ZonedDateTime now, ZonedDateTime refreshTime, Duration maxRefreshFrequency) {
    return now.isAfter(refreshTime.plus(maxRefreshFrequency));
  }

  /** Refreshes the queue. */
  public synchronized void refreshQueue() {
    try {
      // Wait until refreshLock can be acquired.
      refreshLock.lock();

      List<ProcessEntity> unlockedActiveProcesses =
          getUnlockedActiveProcessesExcludingReturnedProcesses(processQueueMaxSize);
      List<ProcessEntity> pendingProcesses =
          getPendingProcessesExcludingReturnedProcesses(
              processQueueMaxSize - unlockedActiveProcesses.size());

      refreshTime = ZonedDateTime.now();
      processQueue.clear();
      processQueue.addAll(unlockedActiveProcesses);
      processQueue.addAll(pendingProcesses);
      processQueueRefreshSize = processQueue.size();
      returnedProcesses.clear();
    } finally {
      refreshLock.unlock();
    }
  }

  /**
   * Returns the number of processes in the queue.
   *
   * @return the number of processes in the queue
   */
  public int getProcessQueueSize() {
    return processQueue.size();
  }

  /**
   * Returns the maximum number of processes in the queue.
   *
   * @return the maximum number of processes in the queue
   */
  public int getProcessQueueMaxSize() {
    return processQueueMaxSize;
  }

  /**
   * Returns the number of processes in the queue after refresh.
   *
   * @return the number of processes in the queue after refresh.
   */
  public int getProcessQueueRefreshSize() {
    return processQueueRefreshSize;
  }
  /**
   * Returns the next available process in the queue if there is one, if the queue is not being
   * refreshed and if the number of currently executing processes is lower than the pipeline
   * parallelism.
   *
   * @param activeProcessCount the number of currently executing processes
   * @return the next available process in the queue if there is one, if the queue is not being
   *     refreshed and if the number of currently executing processes is lower than the pipeline
   *     parallelism.
   */
  public synchronized ProcessEntity nextProcess(int activeProcessCount) {
    // Return null if refreshLock can't be acquired.
    if (refreshLock.tryLock()) {
      try {
        if (activeProcessCount >= pipelineParallelism) {
          return null;
        }
        ProcessEntity processEntity = processQueue.poll();
        if (processEntity != null) {
          returnedProcesses.add(processEntity.getProcessId());
        }
        return processEntity;
      } finally {
        refreshLock.unlock();
      }
    }
    return null;
  }

  private List<ProcessEntity> getUnlockedActiveProcessesExcludingReturnedProcesses(
      int maxProcesses) {
    return processService.getUnlockedActiveProcesses(pipelineName, maxProcesses).stream()
        .filter(e -> !returnedProcesses.contains(e.getProcessId()))
        .collect(Collectors.toList());
  }

  private List<ProcessEntity> getPendingProcessesExcludingReturnedProcesses(int maxProcesses) {
    return processService.getPendingProcesses(pipelineName, maxProcesses).stream()
        .filter(e -> !returnedProcesses.contains(e.getProcessId()))
        .collect(Collectors.toList());
  }
}
