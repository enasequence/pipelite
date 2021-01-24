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

import java.time.ZonedDateTime;
import java.util.UUID;
import lombok.extern.flogger.Flogger;
import org.springframework.util.Assert;
import pipelite.Pipeline;
import pipelite.ProcessSource;
import pipelite.configuration.LauncherConfiguration;
import pipelite.configuration.WebConfiguration;
import pipelite.entity.ProcessEntity;
import pipelite.launcher.process.creator.ProcessCreator;
import pipelite.launcher.process.queue.ProcessQueue;
import pipelite.launcher.process.runner.ProcessRunnerPool;
import pipelite.launcher.process.runner.ProcessRunnerPoolService;
import pipelite.lock.PipeliteLocker;
import pipelite.metrics.PipeliteMetrics;
import pipelite.process.Process;

/**
 * Executes processes in parallel for one pipeline. New process instances are created using {@link
 * Pipeline} for process ids waiting to be executed. New process ids are provided by {@link
 * ProcessSource} or they are inserted into the PIPELITE_PROCESS table.
 */
@Flogger
public class PipeliteLauncher extends ProcessRunnerPoolService {

  private final String pipelineName;
  private final Pipeline pipeline;
  private final ProcessCreator processCreator;
  private final ProcessQueue processQueue;
  private final int processCreateMaxSize;
  private final boolean shutdownIfIdle;
  private final ZonedDateTime startTime;

  public PipeliteLauncher(
      LauncherConfiguration launcherConfiguration,
      PipeliteLocker pipeliteLocker,
      Pipeline pipeline,
      ProcessCreator processCreator,
      ProcessQueue processQueue,
      ProcessRunnerPool pool,
      PipeliteMetrics metrics) {
    super(launcherConfiguration, pipeliteLocker, pool, metrics, processQueue.getLauncherName());
    Assert.notNull(launcherConfiguration, "Missing launcher configuration");
    Assert.notNull(pipeline, "Missing pipeline");
    Assert.notNull(processCreator, "Missing process creator");
    Assert.notNull(processQueue, "Missing process queue");
    this.pipeline = pipeline;
    this.processCreator = processCreator;
    this.processQueue = processQueue;
    this.pipelineName = processQueue.getPipelineName();
    this.processCreateMaxSize = launcherConfiguration.getProcessCreateMaxSize();
    this.shutdownIfIdle = launcherConfiguration.isShutdownIfIdle();
    this.startTime = ZonedDateTime.now();
  }

  @Override
  protected void startUp() {
    super.startUp();
  }

  @Override
  protected void run() {
    if (processQueue.isFillQueue()) {
      processCreator.createProcesses(processCreateMaxSize);
      processQueue.fillQueue();
    }
    while (processQueue.isAvailableProcesses(this.getActiveProcessCount())) {
      runProcess(processQueue.nextAvailableProcess());
    }
  }

  @Override
  protected boolean shutdownIfIdle() {
    return !processQueue.isAvailableProcesses(0)
        && this.getActiveProcessCount() == 0
        && shutdownIfIdle;
  }

  public ZonedDateTime getStartTime() {
    return startTime;
  }

  protected void runProcess(ProcessEntity processEntity) {
    Process process = null;
    try {
      process = PipelineHelper.create(processEntity, pipeline);
    } catch (Exception ex) {
      log.atSevere().withCause(ex).log(
          "Unexpected exception when creating " + pipelineName + " process");
      metrics.pipeline(pipelineName).incrementInternalErrorCount();
    }
    if (process != null) {
      runProcess(pipelineName, process, (p, r) -> {});
    }
  }

  public String getPipelineName() {
    return pipelineName;
  }

  public int getPipelineParallelism() {
    return pipeline.getPipelineParallelism();
  }

  public int getQueuedProcessCount() {
    return processQueue.getQueuedProcessCount();
  }

  public static String getLauncherName(String pipelineName, int port) {
    return pipelineName
        + "@"
        + WebConfiguration.getCanonicalHostName()
        + ":"
        + port
        + ":"
        + UUID.randomUUID();
  }
}
