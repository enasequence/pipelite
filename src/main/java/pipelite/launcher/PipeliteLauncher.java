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
import lombok.extern.flogger.Flogger;
import org.springframework.util.Assert;
import pipelite.Pipeline;
import pipelite.configuration.AdvancedConfiguration;
import pipelite.configuration.ServiceConfiguration;
import pipelite.entity.ProcessEntity;
import pipelite.launcher.process.creator.PrioritizedProcessCreator;
import pipelite.launcher.process.queue.ProcessQueue;
import pipelite.launcher.process.runner.ProcessRunnerPool;
import pipelite.launcher.process.runner.ProcessRunnerPoolService;
import pipelite.lock.PipeliteLocker;
import pipelite.metrics.PipeliteMetrics;
import pipelite.process.Process;
import pipelite.process.ProcessFactory;

/**
 * Executes processes in parallel for one pipeline. New process instances are created using {@link
 * Pipeline} for process ids waiting to be executed. New process ids are provided by {@link
 * ProcessSource} or they are inserted into the PIPELITE_PROCESS table.
 */
@Flogger
public class PipeliteLauncher extends ProcessRunnerPoolService {

  private final String pipelineName;
  private final Pipeline pipeline;
  private final PrioritizedProcessCreator prioritizedProcessCreator;
  private final ProcessQueue processQueue;
  private final int processCreateMaxSize;
  private final boolean shutdownIfIdle;
  private final ZonedDateTime startTime;
  private final String serviceName;

  public PipeliteLauncher(
      ServiceConfiguration serviceConfiguration,
      AdvancedConfiguration advancedConfiguration,
      PipeliteLocker pipeliteLocker,
      Pipeline pipeline,
      PrioritizedProcessCreator prioritizedProcessCreator,
      ProcessQueue processQueue,
      ProcessRunnerPool pool,
      PipeliteMetrics metrics) {
    super(advancedConfiguration, pipeliteLocker, pool, metrics);
    Assert.notNull(advancedConfiguration, "Missing launcher configuration");
    Assert.notNull(pipeline, "Missing pipeline");
    Assert.notNull(prioritizedProcessCreator, "Missing process creator");
    Assert.notNull(processQueue, "Missing process queue");
    this.pipeline = pipeline;
    this.prioritizedProcessCreator = prioritizedProcessCreator;
    this.processQueue = processQueue;
    this.pipelineName = processQueue.getPipelineName();
    this.processCreateMaxSize = advancedConfiguration.getProcessCreateMaxSize();
    this.shutdownIfIdle = advancedConfiguration.isShutdownIfIdle();
    this.startTime = ZonedDateTime.now();
    this.serviceName = serviceConfiguration.getName();
  }

  @Override
  public String getLauncherName() {
    return serviceName + "@pipeline@" + pipelineName;
  }

  @Override
  protected void startUp() {
    super.startUp();
  }

  @Override
  protected void run() {
    if (processQueue.isFillQueue()) {
      prioritizedProcessCreator.createProcesses(processCreateMaxSize);
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
      process = ProcessFactory.create(processEntity, pipeline);
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
}
