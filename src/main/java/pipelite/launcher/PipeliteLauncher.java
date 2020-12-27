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

import io.micrometer.core.instrument.MeterRegistry;
import java.time.ZonedDateTime;
import java.util.function.Supplier;
import lombok.extern.flogger.Flogger;
import org.springframework.util.Assert;
import pipelite.configuration.LauncherConfiguration;
import pipelite.entity.ProcessEntity;
import pipelite.launcher.process.creator.ProcessCreator;
import pipelite.launcher.process.queue.ProcessQueue;
import pipelite.launcher.process.runner.ProcessRunnerMetrics;
import pipelite.launcher.process.runner.ProcessRunnerPool;
import pipelite.launcher.process.runner.ProcessRunnerPoolService;
import pipelite.lock.PipeliteLocker;
import pipelite.process.Process;
import pipelite.process.ProcessFactory;

/**
 * Executes processes in parallel for one pipeline. New process instances are created using {@link
 * pipelite.process.ProcessFactory} for process ids waiting to be executed. New process ids are
 * provided by {@link pipelite.process.ProcessSource} or they are inserted into the PIPELITE_PROCESS
 * table.
 */
@Flogger
public class PipeliteLauncher extends ProcessRunnerPoolService {

  private final String pipelineName;
  private final ProcessFactory processFactory;
  private final ProcessCreator processCreator;
  private final ProcessQueue processQueue;
  private final int processCreateMaxSize;
  private final boolean shutdownIfIdle;
  private final ZonedDateTime startTime;
  private final ProcessRunnerMetrics metrics;

  public PipeliteLauncher(
      LauncherConfiguration launcherConfiguration,
      PipeliteLocker pipeliteLocker,
      ProcessFactory processFactory,
      ProcessCreator processCreator,
      ProcessQueue processQueue,
      Supplier<ProcessRunnerPool> processRunnerPoolSupplier,
      MeterRegistry meterRegistry) {
    super(
        launcherConfiguration,
        pipeliteLocker,
        processQueue.getLauncherName(),
        processRunnerPoolSupplier);
    Assert.notNull(launcherConfiguration, "Missing launcher configuration");
    Assert.notNull(processFactory, "Missing process factory");
    Assert.notNull(processCreator, "Missing process creator");
    Assert.notNull(processQueue, "Missing process queue");
    this.processFactory = processFactory;
    this.processCreator = processCreator;
    this.processQueue = processQueue;
    this.pipelineName = processQueue.getPipelineName();
    this.processCreateMaxSize = launcherConfiguration.getProcessCreateMaxSize();
    this.shutdownIfIdle = launcherConfiguration.isShutdownIfIdle();
    this.startTime = ZonedDateTime.now();
    this.metrics = new ProcessRunnerMetrics(this.pipelineName, meterRegistry);
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

    purgeMetrics();
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
    Process process = ProcessFactory.create(processEntity, processFactory);
    if (process != null) {
      runProcess(
          pipelineName,
          process,
          (p, r) -> metrics.addProcessRunnerResult(p.getProcessEntity().getState(), r));
    } else {
      metrics.addProcessCreationFailed(1);
    }
  }

  public String getPipelineName() {
    return pipelineName;
  }

  public int getProcessParallelism() {
    return processFactory.getProcessParallelism();
  }

  public int getQueuedProcessCount() {
    return processQueue.getQueuedProcessCount();
  }

  public ProcessRunnerMetrics getMetrics() {
    return metrics;
  }

  private void purgeMetrics() {
    metrics.purge();
  }
}
