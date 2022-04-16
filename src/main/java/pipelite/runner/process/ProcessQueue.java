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

import com.google.common.flogger.FluentLogger;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.concurrent.locks.ReentrantLock;
import lombok.Value;
import lombok.experimental.Accessors;
import lombok.extern.flogger.Flogger;
import org.springframework.util.Assert;
import pipelite.Pipeline;
import pipelite.configuration.PipeliteConfiguration;
import pipelite.entity.ProcessEntity;
import pipelite.log.LogKey;
import pipelite.runner.process.creator.ProcessEntityCreator;
import pipelite.service.PipeliteServices;
import pipelite.service.ProcessService;

/**
 * The process queue is created by selecting processes from the database based on process status and
 * priority. The queue creation is an expensive process and the queue should be refreshed as
 * infrequently as possible. However, the queue must be refreshed periodically to prioritise the
 * execution of new high-priority processes. Finally, the queue should always have processes to
 * execute. The following parameters affect the queue refresh frequency: minRefreshFrequency is the
 * minimum frequency for process queue to be refreshed and maxRefreshFrequency is the maximum
 * frequency for process queue to be refreshed.
 */
@Flogger
public class ProcessQueue {

  private static final int MAX_PARALLELISM = 25000;
  private static final float DEFAULT_MIN_QUEUE_SIZE_PARALLELISM_MULTIPLIER = 0.5f;
  private static final int DEFAULT_MAX_QUEUE_SIZE_PARALLELISM_MULTIPLIER = 5;
  static final int MAX_QUEUE_SIZE_PARALLELISM_MULTIPLIER = 50;
  static final float MIN_QUEUE_SIZE_INCREASE = 1.5f;
  static final float MIN_QUEUE_SIZE_FRACTION = 0.5f;

  /** The size of the process queue. */
  @Value
  @Accessors(fluent = true)
  public static class ProcessQueueSize {
    /** The minimum number of processes in the queue before queue refresh is triggered. */
    int min;
    /** The maximum number of processes in the queue after refresh. */
    int max;

    public ProcessQueueSize(int min, int max) {
      this.min = Math.max(1, min);
      this.max = Math.max(1, max);
    }
  }

  private final ProcessService processService;
  private final ProcessEntityCreator processEntityCreator;
  private final String pipelineName;
  private final int pipelineParallelism;
  private final Duration minRefreshFrequency;
  private final Duration maxRefreshFrequency;
  private final Queue<ProcessEntity> processQueue = new ArrayDeque<>();
  private ProcessQueueSize processQueueSize;
  /** The size of the process queue after last refresh before excluding running processes. */
  private int refreshQueueSize = 0;

  private int activeProcessCnt = 0;
  private int pendingProcessCnt = 0;
  private int createdProcessCnt = 0;

  private ZonedDateTime refreshTime;

  /**
   * The refreshQueue and nextProcess methods are called from different threads. The
   * processQueueLock makes sure that the refreshQueue and nextProcess are not accessing the
   * processQueue at the same time.
   */
  private final ReentrantLock processQueueLock = new ReentrantLock();

  /**
   * When the refreshQueue method is called some processes selected from the database may have been
   * returned by nextProcess. These processes are removed from the refreshed queue.
   */
  private final Set<String> returnedProcesses = new HashSet();

  public ProcessQueue(
      PipeliteConfiguration pipeliteConfiguration,
      PipeliteServices pipeliteServices,
      ProcessEntityCreator processEntityCreator,
      Pipeline pipeline) {
    Assert.notNull(pipeliteConfiguration, "Missing pipelite configuration");
    Assert.notNull(pipeliteServices, "Missing pipelite services");
    Assert.notNull(processEntityCreator, "Missing process entity creator");
    Assert.notNull(pipeline, "Missing pipeline");
    Assert.notNull(pipeline.pipelineName(), "Missing pipeline name");
    Assert.notNull(
        pipeline.configurePipeline().pipelineParallelism(), "Missing pipeline parallelism");

    this.processService = pipeliteServices.process();
    this.processEntityCreator = processEntityCreator;
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

    processQueueSize =
        new ProcessQueueSize(
            defaultMinQueueSize(pipelineParallelism), defaultMaxQueueSize(pipelineParallelism));
  }

  public static int defaultMinQueueSize(int pipelineParallelism) {
    return (int) (pipelineParallelism * DEFAULT_MIN_QUEUE_SIZE_PARALLELISM_MULTIPLIER);
  }

  public static int defaultMaxQueueSize(int pipelineParallelism) {
    return pipelineParallelism * DEFAULT_MAX_QUEUE_SIZE_PARALLELISM_MULTIPLIER;
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
    return isRefreshQueue(
        refreshTime,
        minRefreshFrequency,
        maxRefreshFrequency,
        processQueueSize,
        processQueue.size());
  }

  static boolean isRefreshQueue(
      ZonedDateTime refreshTime,
      Duration minRefreshFrequency,
      Duration maxRefreshFrequency,
      ProcessQueueSize processQueueSize,
      int currentQueueSize) {
    ZonedDateTime now = ZonedDateTime.now();
    if (refreshTime == null) {
      // Refresh the queue because it has never been refreshed.
      return true;
    }
    if (isRefreshQueuePremature(now, refreshTime, minRefreshFrequency)) {
      // Do not refresh the queue if it is being refreshed more frequently than
      // the minimum refresh frequency.
      return false;
    }
    if (isRefreshQueueOverdue(now, refreshTime, maxRefreshFrequency)) {
      // Refresh the queue because it has not been refreshed within the maximum refresh frequency.
      return true;
    }
    // Refresh the queue if it has less than the minimum number of processes.
    return currentQueueSize < processQueueSize.min();
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
  public void refreshQueue() {

    // Adjust process queue size.
    try {
      // Wait until processQueueLock can be acquired.
      processQueueLock.tryLock();
      if (refreshTime != null) {
        adjustQueue();
      }
    } finally {
      processQueueLock.unlock();
    }

    // Get processes from database.
    List<ProcessEntity> activeProcesses = getActiveProcesses(processQueueSize.max());
    List<ProcessEntity> pendingProcesses =
        getPendingProcesses(processQueueSize.max() - activeProcesses.size());
    List<ProcessEntity> createdProcesses =
        processEntityCreator.create(
            processQueueSize.max() - activeProcesses.size() - pendingProcesses.size());

    // Refresh process queue.
    try {
      // Wait until processQueueLock can be acquired.
      processQueueLock.tryLock();
      refreshTime = ZonedDateTime.now();
      refreshQueueSize = activeProcesses.size() + pendingProcesses.size() + createdProcesses.size();
      activeProcessCnt = activeProcesses.size();
      pendingProcessCnt = pendingProcesses.size();
      createdProcessCnt = createdProcesses.size();
      processQueue.clear();
      queueProcesses(activeProcesses);
      queueProcesses(pendingProcesses);
      queueProcesses(createdProcesses);
      returnedProcesses.clear();
    } finally {
      processQueueLock.unlock();
    }
  }

  void adjustQueue() {
    Duration currentRefreshFrequency = Duration.between(refreshTime, ZonedDateTime.now());
    processQueueSize =
        adjustQueue(
            pipelineName,
            pipelineParallelism,
            getProcessQueueSize(),
            getRefreshQueueSize(),
            getCurrentQueueSize(),
            maxRefreshFrequency,
            currentRefreshFrequency);
  }

  private void queueProcesses(List<ProcessEntity> processes) {
    processes.stream()
        .filter(p -> !returnedProcesses.contains(p.getProcessId()))
        .forEach(p -> processQueue.add(p));
  }

  /**
   * Returns the process queue size.
   *
   * @return the process queue size.
   */
  public ProcessQueueSize getProcessQueueSize() {
    return processQueueSize;
  }

  /**
   * Returns the number of processes currently in the queue.
   *
   * @return the number of processes currently in the queue
   */
  public int getCurrentQueueSize() {
    return processQueue.size();
  }

  /**
   * Returns the number of processes in the queue after refresh.
   *
   * @return the number of processes in the queue after refresh.
   */
  public int getRefreshQueueSize() {
    return refreshQueueSize;
  }

  int getActiveProcessCnt() {
    return activeProcessCnt;
  }

  int getPendingProcessCnt() {
    return pendingProcessCnt;
  }

  int getCreatedProcessCnt() {
    return createdProcessCnt;
  }

  /**
   * Returns the next available process in the queue if the number of currently executing processes
   * is lower than the pipeline parallelism.
   *
   * @param activeProcessCount the number of currently executing processes
   * @return the next available process in the queue if the number of currently executing processes
   *     is lower than the pipeline parallelism
   */
  public ProcessEntity nextProcess(int activeProcessCount) {
    try {
      // Wait until processQueueLock can be acquired.
      processQueueLock.tryLock();
      if (activeProcessCount >= pipelineParallelism) {
        return null;
      }
      ProcessEntity processEntity = processQueue.poll();
      if (processEntity != null) {
        returnedProcesses.add(processEntity.getProcessId());
      }
      return processEntity;
    } finally {
      processQueueLock.unlock();
    }
  }

  private List<ProcessEntity> getActiveProcesses(int maxProcesses) {
    return processService.getUnlockedActiveProcesses(pipelineName, maxProcesses);
  }

  private List<ProcessEntity> getPendingProcesses(int maxProcesses) {
    return processService.getPendingProcesses(pipelineName, maxProcesses);
  }

  /**
   * Adjusts the queue size. Increases the maximum size if the current queue refresh frequency is
   * shorter than maximum queue refresh frequency and if the queue was filled during last refresh.
   * Increases the minimum size if the queue became empty before it was refreshed and if the queue
   * was filled during last refresh.
   *
   * @param pipelineName the pipeline name
   * @param queueSize the current queue size
   * @param refreshQueueSize the queue size after last refresh
   * @param maxRefreshFrequency the maximum queue refresh frequency
   * @param currentRefreshFrequency the current queue refresh frequency
   * @return the increased maximum queue sizes
   */
  static ProcessQueueSize adjustQueue(
      String pipelineName,
      int pipelineParallelism,
      ProcessQueueSize queueSize,
      int refreshQueueSize,
      int currentQueueSize,
      Duration maxRefreshFrequency,
      Duration currentRefreshFrequency) {

    if (refreshQueueSize < queueSize.max()) {
      // Queue was not filled during last refresh.
      return queueSize;
    }

    boolean adjustQueueSize = false;

    int maxSize = queueSize.max();
    int minSize = queueSize.min();

    if (currentRefreshFrequency.compareTo(maxRefreshFrequency) < 0) {
      // Current queue refresh frequency is shorter than maximum queue refresh frequency.
      // Increase maximum queue size proportionally to the current refresh frequency.
      adjustQueueSize = true;
      int maxSizeLimit = pipelineParallelism * MAX_QUEUE_SIZE_PARALLELISM_MULTIPLIER;
      maxSize =
          Math.min(
              maxSizeLimit,
              (int)
                  (queueSize.max()
                      * (maxRefreshFrequency.toSeconds()
                          / Math.max(1, currentRefreshFrequency.toSeconds()))));
    }

    if (currentQueueSize == 0) {
      // Queue became empty before it was refreshed.
      // Increase minimum queue size by 25% but do not exceed 50% of the maximum queue size.
      adjustQueueSize = true;
      minSize =
          (int)
              Math.ceil(
                  Math.min(minSize * MIN_QUEUE_SIZE_INCREASE, maxSize * MIN_QUEUE_SIZE_FRACTION));
    }

    if (!adjustQueueSize) {
      // Queue size was not adjusted.
      return queueSize;
    }

    ProcessQueueSize newQueueSize = new ProcessQueueSize(minSize, maxSize);
    logContext(log.atInfo(), pipelineName)
        .log(
            "Increased the process queue size from "
                + queueSize.min()
                + ","
                + queueSize.max()
                + " to "
                + newQueueSize.min()
                + ","
                + newQueueSize.max());
    return newQueueSize;
  }

  private static FluentLogger.Api logContext(FluentLogger.Api log, String pipelineName) {
    return log.with(LogKey.PIPELINE_NAME, pipelineName);
  }
}
