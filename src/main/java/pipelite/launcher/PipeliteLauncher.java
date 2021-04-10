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
import java.time.ZonedDateTime;
import java.util.function.Function;
import lombok.extern.flogger.Flogger;
import org.springframework.util.Assert;
import pipelite.Pipeline;
import pipelite.configuration.PipeliteConfiguration;
import pipelite.entity.ProcessEntity;
import pipelite.launcher.process.creator.PrioritizedProcessCreator;
import pipelite.launcher.process.queue.ProcessQueue;
import pipelite.launcher.process.runner.ProcessRunner;
import pipelite.launcher.process.runner.ProcessRunnerPool;
import pipelite.log.LogKey;
import pipelite.metrics.PipeliteMetrics;
import pipelite.process.Process;
import pipelite.process.ProcessFactory;
import pipelite.service.HealthCheckService;
import pipelite.service.InternalErrorService;
import pipelite.service.PipeliteServices;

/**
 * Executes processes in parallel for one pipeline. New process instances are created using for
 * process ids waiting to be executed.
 */
@Flogger
public class PipeliteLauncher extends ProcessRunnerPool {

  private final InternalErrorService internalErrorService;
  private final HealthCheckService healthCheckService;
  private final String pipelineName;
  private final Pipeline pipeline;
  private final ProcessQueue processQueue;
  private final int processCreateMaxSize;
  private final boolean shutdownIfIdle;
  private final ZonedDateTime startTime;

  public PipeliteLauncher(
      PipeliteConfiguration pipeliteConfiguration,
      PipeliteServices pipeliteServices,
      PipeliteMetrics pipeliteMetrics,
      Pipeline pipeline,
      PrioritizedProcessCreator prioritizedProcessCreator,
      ProcessQueue processQueue,
      Function<String, ProcessRunner> processRunnerSupplier) {
    super(
        pipeliteConfiguration,
        pipeliteServices,
        pipeliteMetrics,
        serviceName(pipeliteConfiguration, processQueue),
        processRunnerSupplier);
    Assert.notNull(prioritizedProcessCreator, "Missing process creator");
    this.internalErrorService = pipeliteServices.internalError();
    this.healthCheckService = pipeliteServices.healthCheck();
    this.pipeline = pipeline;
    this.processQueue = processQueue;
    this.pipelineName = processQueue.getPipelineName();
    this.processCreateMaxSize = pipeliteConfiguration.advanced().getProcessCreateMaxSize();
    this.shutdownIfIdle = pipeliteConfiguration.advanced().isShutdownIfIdle();
    this.startTime = ZonedDateTime.now();
    setRunnerCallback(
        () -> {
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
          } catch (Exception ex) {
            // Catching exceptions here in case they have not already been caught.
            internalErrorService.saveInternalError(
                serviceName(), pipelineName, this.getClass(), ex);
          }
        });
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
        runProcess(pipelineName, process, (p, r) -> {});
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
