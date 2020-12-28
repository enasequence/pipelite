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
import pipelite.configuration.LauncherConfiguration;
import pipelite.launcher.PipeliteService;
import pipelite.lock.PipeliteLocker;
import pipelite.process.Process;

/**
 * Abstract base class for executing processes. Extends PipeliteService for lifecycle management.
 * Uses PipeliteLocker for lock management and ProcessRunnerPool for executing processes.
 */
@Flogger
public abstract class ProcessRunnerPoolService extends PipeliteService
    implements ProcessRunnerPool {

  private final PipeliteLocker locker;
  private final ProcessRunnerPool pool;
  private final Duration processLaunchFrequency;
  private boolean shutdown;

  public ProcessRunnerPoolService(
      LauncherConfiguration launcherConfiguration,
      PipeliteLocker locker,
      String launcherName,
      ProcessRunnerPool pool) {
    Assert.notNull(launcherConfiguration, "Missing launcher configuration");
    Assert.notNull(locker, "Missing locker");
    Assert.notNull(pool, "Missing process runner pool");
    this.locker = locker;
    this.pool = pool;
    this.processLaunchFrequency = launcherConfiguration.getProcessLaunchFrequency();
    this.locker.init(launcherName);
  }

  @Override
  protected Scheduler scheduler() {
    return Scheduler.newFixedDelaySchedule(Duration.ZERO, processLaunchFrequency);
  }

  @Override
  protected void startUp() {
    log.atInfo().log("Starting up service: %s", getLauncherName());
    locker.lock();
  }

  @Override
  protected void runOneIteration() {
    if (isActive()) {
      log.atFine().log("Running service: %s", getLauncherName());

      // Renew lock to avoid lock expiry.
      locker.renewLock();

      try {
        run();
      } catch (Exception ex) {
        log.atSevere().withCause(ex).log(
            "Unexpected exception from service: %s", getLauncherName());
      }

      if (pool.getActiveProcessCount() == 0 && shutdownIfIdle()) {
        log.atInfo().log("Stopping idle service: %s", getLauncherName());
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
  public String serviceName() {
    return getLauncherName();
  }

  public String getLauncherName() {
    return locker.getLauncherName();
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
  public void shutDown() {
    try {
      log.atInfo().log("Shutting down service: %s", getLauncherName());
      pool.shutDown();
      log.atInfo().log("Service has been shut down: %s", getLauncherName());
    } finally {
      locker.unlock();
    }
  }
}
