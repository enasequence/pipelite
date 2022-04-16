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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import lombok.extern.flogger.Flogger;
import org.springframework.util.Assert;
import pipelite.Pipeline;
import pipelite.configuration.PipeliteConfiguration;
import pipelite.entity.ProcessEntity;
import pipelite.error.InternalErrorHandler;
import pipelite.log.LogKey;
import pipelite.metrics.PipeliteMetrics;
import pipelite.process.Process;
import pipelite.process.ProcessFactory;
import pipelite.runner.process.ProcessQueue;
import pipelite.runner.process.ProcessQueueFactory;
import pipelite.runner.process.ProcessRunnerFactory;
import pipelite.runner.process.ProcessRunnerPool;
import pipelite.service.PipeliteServices;

/** Executes processes in parallel for one pipeline. */
@Flogger
public class PipelineRunner extends ProcessRunnerPool {

  private final PipeliteServices pipeliteServices;
  private final String pipelineName;
  private final Pipeline pipeline;
  private final ProcessQueueFactory processQueueFactory;
  private final AtomicReference<ProcessQueue> processQueue = new AtomicReference<>();
  private final ZonedDateTime startTime;
  private AtomicBoolean activeRefreshQueue = new AtomicBoolean();
  private InternalErrorHandler internalErrorHandler;

  public PipelineRunner(
      PipeliteConfiguration pipeliteConfiguration,
      PipeliteServices pipeliteServices,
      PipeliteMetrics pipeliteMetrics,
      Pipeline pipeline,
      ProcessQueueFactory processQueueFactory,
      ProcessRunnerFactory processRunnerFactory) {
    super(
        pipeliteConfiguration,
        pipeliteServices,
        pipeliteMetrics,
        serviceName(pipeliteConfiguration, pipeline.pipelineName()),
        processRunnerFactory);
    Assert.notNull(pipeline, "Missing pipeline");
    Assert.notNull(processQueueFactory, "Missing process queue factory");
    this.pipeliteServices = pipeliteServices;
    this.pipeline = pipeline;
    this.processQueueFactory = processQueueFactory;
    this.pipelineName = pipeline.pipelineName();
    this.startTime = ZonedDateTime.now();
    this.internalErrorHandler =
        new InternalErrorHandler(
            pipeliteServices.internalError(), serviceName(), pipelineName, this);
  }

  // From AbstractScheduledService.
  @Override
  public void runOneIteration() {
    internalErrorHandler.execute(
        () -> {
          if (!pipeliteServices.healthCheck().isHealthy()) {
            logContext(log.atSevere())
                .log("Waiting data source to be healthy before starting new processes");
            return;
          }
          refreshQueue();
          runProcesses();
          // Must call ProcessRunnerPool.runOneIteration()
          super.runOneIteration();
        });
  }

  private void refreshQueue() {
    if (processQueue.get() == null || processQueue.get().isRefreshQueue()) {
      if (activeRefreshQueue.compareAndSet(false, true)) {
        pipeliteServices
            .executor()
            .refreshQueue()
            .execute(
                () -> {
                  internalErrorHandler.execute(
                      () -> {
                        ProcessQueue p = processQueue.get();
                        boolean firstRefresh = p == null;
                        if (firstRefresh) {
                          // Create process queue.
                          // The process queue creation is intentionally deferred to
                          // happen here so that we will only assign the process queue
                          // after it has been refreshed once and processes have been
                          // created if needed.
                          p = processQueueFactory.create(pipeline);
                        }

                        // Refresh process queue.
                        p.refreshQueue();

                        if (firstRefresh) {
                          processQueue.set(p);
                        }
                      });
                  activeRefreshQueue.set(false);
                });
      }
    }
  }

  private void runProcesses() {
    if (processQueue.get() == null) {
      return;
    }
    while (true) {
      ProcessEntity processEntity = processQueue.get().nextProcess(this.getActiveProcessCount());
      if (processEntity == null) {
        return;
      }
      runProcess(processEntity);
    }
  }

  private static String serviceName(
      PipeliteConfiguration pipeliteConfiguration, String pipelineName) {
    return pipeliteConfiguration.service().getName() + "@pipeline@" + pipelineName;
  }

  @Override
  public boolean isIdle() {
    return processQueue.get() != null
        && processQueue.get().getCurrentQueueSize() == 0
        && super.isIdle();
  }

  public ZonedDateTime getStartTime() {
    return startTime;
  }

  protected void runProcess(ProcessEntity processEntity) {
    internalErrorHandler.execute(
        () -> {
          Process process = ProcessFactory.create(processEntity, pipeline);
          runProcess(pipelineName, process, (p) -> {});
        });
  }

  public String getPipelineName() {
    return pipelineName;
  }

  public int getPipelineParallelism() {
    return pipeline.configurePipeline().pipelineParallelism();
  }

  public int getQueuedProcessCount() {
    if (processQueue.get() == null) {
      return 0;
    }
    return processQueue.get().getCurrentQueueSize();
  }

  private FluentLogger.Api logContext(FluentLogger.Api log) {
    return log.with(LogKey.PROCESS_RUNNER_NAME, serviceName())
        .with(LogKey.PIPELINE_NAME, pipelineName);
  }
}
