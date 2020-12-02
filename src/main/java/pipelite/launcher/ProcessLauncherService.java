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

import com.google.common.util.concurrent.AbstractScheduledService;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import lombok.extern.flogger.Flogger;
import org.springframework.util.Assert;
import pipelite.configuration.LauncherConfiguration;
import pipelite.launcher.lock.PipeliteLocker;

/**
 * Abstract base class for process execution services. They use AbstractScheduledService for
 * lifecycle management, PipeliteLocker for lock management, and ProcessLauncherPool for executing
 * processes.
 */
@Flogger
public abstract class ProcessLauncherService extends AbstractScheduledService {

  private final PipeliteLocker locker;
  private final ProcessLauncherPoolFactory processLauncherPoolFactory;
  private final Duration processLaunchFrequency;
  private ProcessLauncherPool pool;
  private boolean shutdown;

  public ProcessLauncherService(
      LauncherConfiguration launcherConfiguration,
      PipeliteLocker locker,
      ProcessLauncherPoolFactory processLauncherPoolFactory) {
    Assert.notNull(launcherConfiguration, "Missing launcher configuration");
    Assert.notNull(locker, "Missing locker");
    Assert.notNull(processLauncherPoolFactory, "Missing process launcher pool factory");
    this.locker = locker;
    this.processLauncherPoolFactory = processLauncherPoolFactory;
    this.processLaunchFrequency = launcherConfiguration.getProcessLaunchFrequency();
  }

  @Override
  protected Scheduler scheduler() {
    return Scheduler.newFixedDelaySchedule(Duration.ZERO, processLaunchFrequency);
  }

  @Override
  protected void startUp() {
    log.atInfo().log("Starting up launcher: %s", getLauncherName());
    locker.lock();
    pool = processLauncherPoolFactory.apply(locker);
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

      if (pool.size() == 0 && shutdownIfIdle()) {
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

  public List<ProcessLauncher> getProcessLaunchers() {
    if (pool == null) {
      return Collections.emptyList();
    }
    return pool.get();
  }

  protected ProcessLauncherPool getPool() {
    return pool;
  }
}
