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
import java.time.Duration;
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
import pipelite.runner.process.creator.ProcessCreator;
import pipelite.service.PipeliteServices;

/** Executes processes in parallel for one pipeline. */
@Flogger
public class PipelineRunner extends ProcessRunnerPool {

  private final PipeliteServices pipeliteServices;
  private final String pipelineName;
  private final Pipeline pipeline;
  private final ProcessCreator processCreator;
  private final ProcessQueueFactory processQueueFactory;
  private final AtomicReference<ProcessQueue> processQueue = new AtomicReference<>();
  private final Duration minReplenishFrequency;
  private final ZonedDateTime startTime;
  private ZonedDateTime replenishTime;
  private AtomicBoolean activeRefreshQueue = new AtomicBoolean();
  private AtomicBoolean activeReplenishQueue = new AtomicBoolean();
  private InternalErrorHandler internalErrorHandler;

  public PipelineRunner(
      PipeliteConfiguration pipeliteConfiguration,
      PipeliteServices pipeliteServices,
      PipeliteMetrics pipeliteMetrics,
      Pipeline pipeline,
      ProcessCreator processCreator,
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
    Assert.notNull(processCreator, "Missing process creator");
    this.pipeliteServices = pipeliteServices;
    this.pipeline = pipeline;
    this.processCreator = processCreator;
    this.processQueueFactory = processQueueFactory;
    this.pipelineName = pipeline.pipelineName();
    this.minReplenishFrequency =
        pipeliteConfiguration.advanced().getProcessQueueMinReplenishFrequency();
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
          if (!pipeliteServices.healthCheck().isDataSourceHealthy()) {
            logContext(log.atSevere())
                .log("Waiting data source to be healthy before starting new processes");
            return;
          }
          refreshQueue();
          replenishQueue();
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

                        if (p.getProcessQueueSize() == 0) {
                          // The process queue is empty.
                          // Create new processes.
                          int createdProcessCount =
                              processCreator.createProcesses(p.getProcessQueueMaxSize());
                          replenishTime = ZonedDateTime.now();
                          if (createdProcessCount > 0) {
                            // Refresh process queue.
                            p.refreshQueue();
                          }
                        }

                        if (firstRefresh) {
                          processQueue.set(p);
                        }
                      });
                  activeRefreshQueue.set(false);
                });
      }
    }
  }

  private void replenishQueue() {
    if (processQueue.get() != null
        && (replenishTime == null
            || replenishTime.plus(minReplenishFrequency).isBefore(ZonedDateTime.now()))) {
      if (activeReplenishQueue.compareAndSet(false, true)) {
        pipeliteServices
            .executor()
            .replenishQueue()
            .execute(
                () -> {
                  internalErrorHandler.execute(
                      () -> {
                        // Create new processes.
                        processCreator.createProcesses(processQueue.get().getProcessQueueMaxSize());
                        replenishTime = ZonedDateTime.now();
                      });
                  activeReplenishQueue.set(false);
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
        && processQueue.get().getProcessQueueSize() == 0
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
    return processQueue.get().getProcessQueueSize();
  }

  private FluentLogger.Api logContext(FluentLogger.Api log) {
    return log.with(LogKey.PROCESS_RUNNER_NAME, serviceName())
        .with(LogKey.PIPELINE_NAME, pipelineName);
  }
}
