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

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;
import lombok.extern.flogger.Flogger;
import org.springframework.util.Assert;
import pipelite.configuration.LauncherConfiguration;
import pipelite.lock.PipeliteLocker;
import pipelite.process.Process;

/**
 * Abstract base class for process execution services. They use AbstractScheduledService for
 * lifecycle management, PipeliteLocker for lock management, and ProcessLauncherPool for executing
 * processes.
 */
@Flogger
public abstract class ProcessLauncherService extends PipeliteService {

  private final PipeliteLocker locker;
  private final Supplier<ProcessLauncherPool> processLauncherPoolSupplier;
  private final Duration processLaunchFrequency;
  private ProcessLauncherPool pool;
  private boolean shutdown;

  public ProcessLauncherService(
      LauncherConfiguration launcherConfiguration,
      PipeliteLocker locker,
      String launcherName,
      Supplier<ProcessLauncherPool> processLauncherPoolSupplier) {
    Assert.notNull(launcherConfiguration, "Missing launcher configuration");
    Assert.notNull(locker, "Missing locker");
    Assert.notNull(processLauncherPoolSupplier, "Missing process launcher pool supplier");
    this.locker = locker;
    this.processLauncherPoolSupplier = processLauncherPoolSupplier;
    this.processLaunchFrequency = launcherConfiguration.getProcessLaunchFrequency();
    this.locker.init(launcherName);
  }

  @Override
  protected Scheduler scheduler() {
    return Scheduler.newFixedDelaySchedule(Duration.ZERO, processLaunchFrequency);
  }

  @Override
  protected void startUp() {
    log.atInfo().log("Starting up launcher: %s", getLauncherName());
    locker.lock();
    pool = processLauncherPoolSupplier.get();
  }

  @Override
  protected void runOneIteration() {
    if (isActive()) {
      log.atInfo().log("Running launcher: %s", getLauncherName());

      // Renew lock to avoid lock expiry.
      locker.renewLock();

      // TODO: review exception handling policy
      try {
        run();
      } catch (Exception ex) {
        log.atSevere().withCause(ex).log(
            "Unexpected exception from launcher: %s", getLauncherName());
      }

      if (pool.activeProcessCount() == 0 && shutdownIfIdle()) {
        log.atInfo().log("Stopping idle launcher: %s", getLauncherName());
        shutdown = true;
        stopAsync();
      }
    }
  }

  protected boolean isActive() {
    return isRunning() && !shutdown;
  }

  @Override
  protected void shutDown() {
    try {
      log.atInfo().log("Shutting down launcher: %s", getLauncherName());
      pool.shutDown();
      log.atInfo().log("Launcher has been shut down: %s", getLauncherName());
    } finally {
      locker.unlock();
    }
  }

  /**
   * Runs one iteration of the service. If an exception is thrown the exception will be logged and
   * run will be called again after a fixed delay.
   */
  protected abstract void run() throws Exception;

  /**
   * Allows an idle launcher to be shut down.
   *
   * @return true if a launcher with no running processes should be shut down
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

  /**
   * Runs a process.
   *
   * @param pipelineName the pipeline name
   * @param process the process
   * @param callback the process execution callback
   */
  protected void run(String pipelineName, Process process, ProcessLauncherPoolCallback callback) {
    pool.run(pipelineName, process, locker, callback);
  }

  /**
   * Returns true if there are any active processes for the pipeline.
   *
   * @param pipelineName the pipeline name
   * @return true if there are any active processes for the pipeline.
   */
  public boolean isPipelineActive(String pipelineName) {
    if (pool == null) {
      return false;
    }
    return pool.isPipelineActive(pipelineName);
  }

  /**
   * Returns the number of active processes.
   *
   * @return the number of active processes.
   */
  public int getActiveProcessCount() {
    if (pool == null) {
      return 0;
    }
    return pool.activeProcessCount();
  }

  public List<ProcessLauncher> getProcessLaunchers() {
    if (pool == null) {
      return Collections.emptyList();
    }
    return pool.get();
  }
}
