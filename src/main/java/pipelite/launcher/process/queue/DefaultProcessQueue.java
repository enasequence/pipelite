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
package pipelite.launcher.process.queue;

import com.google.common.flogger.FluentLogger;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.extern.flogger.Flogger;
import org.springframework.util.Assert;
import pipelite.configuration.LauncherConfiguration;
import pipelite.configuration.WebConfiguration;
import pipelite.entity.ProcessEntity;
import pipelite.log.LogKey;
import pipelite.service.ProcessService;

@Flogger
public class DefaultProcessQueue implements ProcessQueue {

  private final ProcessService processService;
  private final String launcherName;
  private final String pipelineName;
  private final Duration processQueueMaxRefreshFrequency;
  private final Duration processQueueMinRefreshFrequency;
  private final int processQueueMaxSize;
  private final int pipelineParallelism;
  private final List<ProcessEntity> processQueue = Collections.synchronizedList(new ArrayList<>());
  private AtomicInteger processQueueIndex = new AtomicInteger();
  private ZonedDateTime processQueueMaxValidUntil = ZonedDateTime.now();
  private ZonedDateTime processQueueMinValidUntil = ZonedDateTime.now();

  public DefaultProcessQueue(
      WebConfiguration webConfiguration,
      LauncherConfiguration launcherConfiguration,
      ProcessService processService,
      String pipelineName,
      int pipelineParallelism) {
    Assert.notNull(launcherConfiguration, "Missing launcher configuration");
    Assert.notNull(pipelineName, "Missing pipeline name");
    this.processService = processService;
    this.launcherName =
        LauncherConfiguration.getLauncherName(pipelineName, webConfiguration.getPort());
    this.pipelineName = pipelineName;
    this.processQueueMaxRefreshFrequency =
        launcherConfiguration.getProcessQueueMaxRefreshFrequency();
    this.processQueueMinRefreshFrequency =
        launcherConfiguration.getProcessQueueMinRefreshFrequency();
    this.processQueueMaxSize = launcherConfiguration.getProcessQueueMaxSize();
    this.pipelineParallelism = (pipelineParallelism < 1) ? 1 : pipelineParallelism;
  }

  @Override
  public String getLauncherName() {
    return launcherName;
  }

  @Override
  public String getPipelineName() {
    return pipelineName;
  }

  @Override
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

  @Override
  public int fillQueue() {
    if (!isFillQueue()) {
      return 0;
    }
    int processCnt = 0;
    logContext(log.atInfo()).log("Queuing processes");
    // Clear process queue.
    processQueueIndex.set(0);
    processQueue.clear();

    // First add unlocked active processes. Asynchronous active processes may be able to continue
    // execution.
    List<ProcessEntity> availableActiveProcesses = getAvailableActiveProcesses();
    processCnt += availableActiveProcesses.size();
    processQueue.addAll(availableActiveProcesses);

    // Then add new processes.
    List<ProcessEntity> pendingProcesses = getPendingProcesses();
    processCnt += pendingProcesses.size();
    processQueue.addAll(pendingProcesses);
    processQueueMaxValidUntil = ZonedDateTime.now().plus(processQueueMaxRefreshFrequency);
    processQueueMinValidUntil = ZonedDateTime.now().plus(processQueueMinRefreshFrequency);
    return processCnt;
  }

  @Override
  public boolean isAvailableProcesses(int activeProcesses) {
    return processQueueIndex.get() < processQueue.size() && activeProcesses < pipelineParallelism;
  }

  @Override
  public int getQueuedProcessCount() {
    return processQueue.size() - processQueueIndex.get();
  }

  @Override
  public ProcessEntity nextAvailableProcess() {
    return processQueue.get(processQueueIndex.getAndIncrement());
  }

  public List<ProcessEntity> getAvailableActiveProcesses() {
    return processService.getAvailableActiveProcesses(pipelineName, processQueueMaxSize);
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

  private FluentLogger.Api logContext(FluentLogger.Api log) {
    return log.with(LogKey.LAUNCHER_NAME, launcherName).with(LogKey.PIPELINE_NAME, pipelineName);
  }
}
