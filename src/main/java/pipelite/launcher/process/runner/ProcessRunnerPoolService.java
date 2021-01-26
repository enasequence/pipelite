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
package pipelite.launcher.process.runner;

import java.time.Duration;
import java.util.List;
import lombok.extern.flogger.Flogger;
import org.springframework.util.Assert;
import pipelite.configuration.AdvancedConfiguration;
import pipelite.launcher.PipeliteService;
import pipelite.lock.PipeliteLocker;
import pipelite.metrics.PipeliteMetrics;
import pipelite.process.Process;

/** Abstract base class for executing processes. */
@Flogger
public abstract class ProcessRunnerPoolService extends PipeliteService
    implements ProcessRunnerPool {

  private final PipeliteLocker locker;
  private final ProcessRunnerPool pool;
  protected final PipeliteMetrics metrics;
  private final Duration processRunnerFrequency;
  private boolean shutdown;

  public ProcessRunnerPoolService(
      AdvancedConfiguration advancedConfiguration,
      PipeliteLocker locker,
      ProcessRunnerPool pool,
      PipeliteMetrics metrics) {
    Assert.notNull(advancedConfiguration, "Missing launcher configuration");
    Assert.notNull(locker, "Missing locker");
    Assert.notNull(pool, "Missing process runner pool");
    Assert.notNull(metrics, "Missing metrics");
    this.locker = locker;
    this.pool = pool;
    this.metrics = metrics;
    this.processRunnerFrequency = advancedConfiguration.getProcessRunnerFrequency();
  }

  @Override
  protected Scheduler scheduler() {
    return Scheduler.newFixedDelaySchedule(Duration.ZERO, processRunnerFrequency);
  }

  @Override
  protected void startUp() {
    log.atInfo().log("Starting up service: %s", serviceName());
  }

  @Override
  protected void runOneIteration() {
    if (isActive()) {
      log.atFine().log("Running service: %s", serviceName());

      try {
        run();
        metrics.setRunningProcessesCount(pool.getActiveProcessRunners());
      } catch (Exception ex) {
        log.atSevere().withCause(ex).log("Unexpected exception from service: %s", serviceName());
      }

      if (pool.getActiveProcessCount() == 0 && shutdownIfIdle()) {
        log.atInfo().log("Stopping idle service: %s", serviceName());
        shutdown = true;
        stopAsync();
      }
    }
  }

  /**
   * Returns true if the service is active.
   *
   * @return true if the service is active.
   */
  protected boolean isActive() {
    return isRunning() && !shutdown;
  }

  /**
   * Implement to runs one iteration of the service. If an exception is thrown the exception will be
   * logged and run will be called again after a fixed delay.
   */
  protected abstract void run() throws Exception;

  /**
   * Override to allow an idle launcher to be shut down.
   *
   * @return true if a launcher is idle and can be shut down
   */
  protected boolean shutdownIfIdle() {
    return false;
  }

  @Override
  public void runProcess(String pipelineName, Process process, ProcessRunnerCallback callback) {
    pool.runProcess(pipelineName, process, callback);
  }

  @Override
  public int getActiveProcessCount() {
    return pool.getActiveProcessCount();
  }

  @Override
  public List<ProcessRunner> getActiveProcessRunners() {
    return pool.getActiveProcessRunners();
  }

  @Override
  public boolean isPipelineActive(String pipelineName) {
    return pool.isPipelineActive(pipelineName);
  }

  @Override
  public boolean isProcessActive(String pipelineName, String processId) {
    return pool.isProcessActive(pipelineName, processId);
  }

  @Override
  public void terminate() {
    pool.terminate();
  }

  @Override
  public void shutDown() {
    log.atInfo().log("Shutting down service: %s", serviceName());
    pool.shutDown();
    log.atInfo().log("Service has been shut down: %s", serviceName());
  }
}
