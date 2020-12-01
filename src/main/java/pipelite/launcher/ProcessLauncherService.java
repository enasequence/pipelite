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
import lombok.extern.flogger.Flogger;
import org.springframework.util.Assert;
import pipelite.configuration.LauncherConfiguration;
import pipelite.launcher.lock.PipeliteLocker;
import pipelite.process.Process;
import pipelite.service.*;

import java.time.Duration;
import java.util.List;
import java.util.function.BiFunction;

/**
 * Abstract base class for process execution services. They use AbstractScheduledService for
 * lifecycle management, PipeliteLocker for lock management, and ProcessLauncherPool for executing
 * processes.
 */
@Flogger
public abstract class ProcessLauncherService extends AbstractScheduledService {

  private final LockService lockService;
  private final String launcherName;
  private final Duration processLaunchFrequency;
  private final BiFunction<String, Process, ProcessLauncher> factory;
  private final PipeliteLocker locker;
  private ProcessLauncherPool pool;
  private boolean shutdown;

  public ProcessLauncherService(
      LauncherConfiguration launcherConfiguration,
      LockService lockService,
      String launcherName,
      BiFunction<String, Process, ProcessLauncher> factory) {
    Assert.notNull(launcherConfiguration, "Missing launcher configuration");
    Assert.notNull(lockService, "Missing lock service");
    Assert.notNull(launcherName, "Missing launcher name");
    Assert.notNull(factory, "Missing process launcher factory");
    this.lockService = lockService;
    this.launcherName = launcherName;
    this.processLaunchFrequency = launcherConfiguration.getProcessLaunchFrequency();
    this.locker = new PipeliteLocker(lockService, launcherName);
    this.factory = factory;
  }

  @Override
  protected Scheduler scheduler() {
    return Scheduler.newFixedDelaySchedule(Duration.ZERO, processLaunchFrequency);
  }

  @Override
  protected void startUp() {
    log.atInfo().log("Starting up launcher: %s", launcherName);
    locker.lock();
    pool = new ProcessLauncherPool(lockService, factory, locker.getLock());
  }

  /** Starts the shutdown process. */
  protected void startShutdown() {
    shutdown = true;
    stopAsync();
  }

  @Override
  protected void runOneIteration() throws Exception {
    if (isRunning() && !shutdown) {
      log.atInfo().log("Running launcher: %s", launcherName);
      // Renew lock to avoid lock expiry.
      locker.renewLock();
      run();
    }
  }

  @Override
  protected void shutDown() throws InterruptedException {
    try {
      log.atInfo().log("Shutting down launcher: %s", launcherName);
      pool.shutDown();
      log.atInfo().log("Launcher has been shut down: %s", launcherName);
    } finally {
      locker.unlock();
    }
  }

  /**
   * Runs one iteration of the service. If an exception is thrown the service will transition to
   * failed state and this method will no longer be called.
   */
  protected abstract void run() throws Exception;

  @Override
  public String serviceName() {
    return launcherName;
  }

  public String getLauncherName() {
    return launcherName;
  }

  public List<ProcessLauncher> getProcessLaunchers() {
    return pool.get();
  }

  protected ProcessLauncherPool getPool() {
    return pool;
  }
}
