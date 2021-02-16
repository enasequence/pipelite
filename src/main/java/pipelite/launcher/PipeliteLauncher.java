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
import pipelite.entity.ProcessEntity;
import pipelite.launcher.process.creator.PrioritizedProcessCreator;
import pipelite.launcher.process.queue.ProcessQueue;
import pipelite.launcher.process.runner.ProcessRunnerPool;
import pipelite.launcher.process.runner.ProcessRunnerPoolService;
import pipelite.process.Process;
import pipelite.process.ProcessFactory;
import pipelite.service.HealthCheckService;
import pipelite.service.InternalErrorService;

/**
 * Executes processes in parallel for one pipeline. New process instances are created using for
 * process ids waiting to be executed.
 */
@Flogger
public class PipeliteLauncher extends ProcessRunnerPoolService {

  private final InternalErrorService internalErrorService;
  private final HealthCheckService healthCheckService;
  private final String pipelineName;
  private final Pipeline pipeline;
  private final PrioritizedProcessCreator prioritizedProcessCreator;
  private final ProcessQueue processQueue;
  private final int processCreateMaxSize;
  private final boolean shutdownIfIdle;
  private final ZonedDateTime startTime;
  private final String serviceName;

  public PipeliteLauncher(
      PipeliteConfiguration pipeliteConfiguration,
      PipeliteServices pipeliteServices,
      Pipeline pipeline,
      PrioritizedProcessCreator prioritizedProcessCreator,
      ProcessQueue processQueue,
      ProcessRunnerPool pool) {
    super(pipeliteConfiguration, pool);
    Assert.notNull(pipeliteConfiguration, "Missing configuration");
    Assert.notNull(pipeliteServices, "Missing services");
    Assert.notNull(pipeline, "Missing pipeline");
    Assert.notNull(prioritizedProcessCreator, "Missing process creator");
    Assert.notNull(processQueue, "Missing process queue");
    this.internalErrorService = pipeliteServices.internalError();
    this.healthCheckService = pipeliteServices.healthCheckService();
    this.pipeline = pipeline;
    this.prioritizedProcessCreator = prioritizedProcessCreator;
    this.processQueue = processQueue;
    this.pipelineName = processQueue.getPipelineName();
    this.processCreateMaxSize = pipeliteConfiguration.advanced().getProcessCreateMaxSize();
    this.shutdownIfIdle = pipeliteConfiguration.advanced().isShutdownIfIdle();
    this.startTime = ZonedDateTime.now();
    this.serviceName = pipeliteConfiguration.service().getName();
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
    try {
      if (!healthCheckService.databaseHealthy()) {
        log.atSevere().log(
            "Waiting database to be healthy to start new pipelines: " + pipelineName);
        return;
      }

      if (processQueue.isFillQueue()) {
        prioritizedProcessCreator.createProcesses(processCreateMaxSize);
        processQueue.fillQueue();
      }
      while (processQueue.isAvailableProcesses(this.getActiveProcessCount())) {
        runProcess(processQueue.nextAvailableProcess());
      }
    } catch (Exception ex) {
      // Catching exceptions here in case they have not already been caught.
      internalErrorService.saveInternalError(serviceName, pipelineName, this.getClass(), ex);
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
    try {
      Process process = ProcessFactory.create(processEntity, pipeline);
      if (process != null) {
        runProcess(pipelineName, process, (p, r) -> {});
      }
    } catch (Exception ex) {
      // Catching exceptions here to allow other processes to continue execution.
      internalErrorService.saveInternalError(serviceName, pipelineName, this.getClass(), ex);
    }
  }

  public String getPipelineName() {
    return pipelineName;
  }

  public int getPipelineParallelism() {
    return pipeline.configurePipeline().pipelineParallelism();
  }

  public int getQueuedProcessCount() {
    return processQueue.getQueuedProcessCount();
  }
}
