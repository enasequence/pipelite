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

import com.google.common.flogger.FluentLogger;
import lombok.extern.flogger.Flogger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import pipelite.configuration.LauncherConfiguration;
import pipelite.entity.LauncherLockEntity;
import pipelite.lock.PipeliteLocker;
import pipelite.log.LogKey;
import pipelite.service.LockService;

@Component
@Flogger
/** Removes expired launcher and process locks. */
public class PipeliteUnlocker extends PipeliteService {

  private final LauncherConfiguration launcherConfiguration;
  private final LockService lockService;
  private final Duration unlockFrequency;
  private final String unlockerName;

  public PipeliteUnlocker(
      @Autowired LauncherConfiguration launcherConfiguration, @Autowired LockService lockService) {
    this.launcherConfiguration = launcherConfiguration;
    this.lockService = lockService;
    this.unlockFrequency = launcherConfiguration.getPipelineUnlockFrequency();
    this.unlockerName = LauncherConfiguration.getUnlockerName(launcherConfiguration);
  }

  @Override
  protected Scheduler scheduler() {
    return Scheduler.newFixedDelaySchedule(Duration.ZERO, unlockFrequency);
  }

  @Override
  protected void runOneIteration() {
    if (!isRunning()) {
      return;
    }
    removeExpiredLocks();
  }

  @Override
  public String serviceName() {
    return unlockerName;
  }

  public void removeExpiredLocks() {
    for (LauncherLockEntity launcherLock : lockService.getExpiredLauncherLocks()) {
      logContext(log.atInfo(), launcherLock.getLauncherName()).log("Removing expired locks");
      PipeliteLocker.unlock(lockService, launcherLock);
    }
  }

  private FluentLogger.Api logContext(FluentLogger.Api log, String launcherName) {
    return log.with(LogKey.UNLOCKER_NAME, unlockerName).with(LogKey.LAUNCHER_NAME, launcherName);
  }
}
