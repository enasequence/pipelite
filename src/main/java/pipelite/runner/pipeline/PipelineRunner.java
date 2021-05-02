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
package pipelite.runner.pipeline;

import com.google.common.flogger.FluentLogger;
import java.time.ZonedDateTime;
import lombok.extern.flogger.Flogger;
import org.springframework.util.Assert;
import pipelite.Pipeline;
import pipelite.configuration.PipeliteConfiguration;
import pipelite.entity.ProcessEntity;
import pipelite.log.LogKey;
import pipelite.metrics.PipeliteMetrics;
import pipelite.process.Process;
import pipelite.process.ProcessFactory;
import pipelite.runner.process.ProcessRunnerFactory;
import pipelite.runner.process.ProcessRunnerPool;
import pipelite.runner.process.creator.PrioritizedProcessCreator;
import pipelite.runner.process.queue.ProcessQueue;
import pipelite.service.HealthCheckService;
import pipelite.service.InternalErrorService;
import pipelite.service.PipeliteServices;

/** Executes processes in parallel for one pipeline. */
@Flogger
public class PipelineRunner extends ProcessRunnerPool {

  private final InternalErrorService internalErrorService;
  private final HealthCheckService healthCheckService;
  private final String pipelineName;
  private final Pipeline pipeline;
  private final PrioritizedProcessCreator prioritizedProcessCreator;
  private final ProcessQueue processQueue;
  private final int processCreateMaxSize;
  private final boolean shutdownIfIdle;
  private final ZonedDateTime startTime;

  public PipelineRunner(
      PipeliteConfiguration pipeliteConfiguration,
      PipeliteServices pipeliteServices,
      PipeliteMetrics pipeliteMetrics,
      Pipeline pipeline,
      PrioritizedProcessCreator prioritizedProcessCreator,
      ProcessQueue processQueue,
      ProcessRunnerFactory processRunnerFactory) {
    super(
        pipeliteConfiguration,
        pipeliteServices,
        pipeliteMetrics,
        serviceName(pipeliteConfiguration, processQueue),
        processRunnerFactory);
    Assert.notNull(prioritizedProcessCreator, "Missing process creator");
    this.internalErrorService = pipeliteServices.internalError();
    this.healthCheckService = pipeliteServices.healthCheck();
    this.pipeline = pipeline;
    this.prioritizedProcessCreator = prioritizedProcessCreator;
    this.processQueue = processQueue;
    this.pipelineName = processQueue.getPipelineName();
    this.processCreateMaxSize = pipeliteConfiguration.advanced().getProcessCreateMaxSize();
    this.shutdownIfIdle = pipeliteConfiguration.advanced().isShutdownIfIdle();
    this.startTime = ZonedDateTime.now();
  }

  // From AbstractScheduledService.
  @Override
  public void runOneIteration() {
    try {
      if (!healthCheckService.isDataSourceHealthy()) {
        logContext(log.atSevere())
            .log("Waiting data source to be healthy before starting new processes");
        return;
      }

      if (processQueue.isFillQueue()) {
        prioritizedProcessCreator.createProcesses(processCreateMaxSize);
        processQueue.fillQueue();
      }
      while (processQueue.isAvailableProcesses(this.getActiveProcessCount())) {
        runProcess(processQueue.nextAvailableProcess());
      }

      // Must call ProcessRunnerPool.runOneIteration()
      super.runOneIteration();

    } catch (Exception ex) {
      // Catching exceptions here in case they have not already been caught.
      internalErrorService.saveInternalError(serviceName(), pipelineName, this.getClass(), ex);
    }
  }

  private static String serviceName(
      PipeliteConfiguration pipeliteConfiguration, ProcessQueue processQueue) {
    return pipeliteConfiguration.service().getName()
        + "@pipeline@"
        + processQueue.getPipelineName();
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
        runProcess(pipelineName, process, (p) -> {});
      }
    } catch (Exception ex) {
      // Catching exceptions here to allow other processes to continue execution.
      internalErrorService.saveInternalError(serviceName(), pipelineName, this.getClass(), ex);
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

  private FluentLogger.Api logContext(FluentLogger.Api log) {
    return log.with(LogKey.PROCESS_RUNNER_NAME, serviceName())
        .with(LogKey.PIPELINE_NAME, pipelineName);
  }
}
