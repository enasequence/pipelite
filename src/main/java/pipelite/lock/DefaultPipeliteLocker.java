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
package pipelite.lock;

import lombok.extern.flogger.Flogger;
import org.springframework.util.Assert;
import pipelite.entity.LauncherLockEntity;
import pipelite.launcher.process.runner.ProcessRunnerType;
import pipelite.service.LockService;

@Flogger
/** The default launcher locker. */
public class DefaultPipeliteLocker implements PipeliteLocker {

  private final LockService lockService;
  private final ProcessRunnerType launcherType;
  private String launcherName;
  private LauncherLockEntity lock;

  public DefaultPipeliteLocker(LockService lockService, ProcessRunnerType launcherType) {
    Assert.notNull(lockService, "Missing locker service");
    Assert.notNull(launcherType, "Missing process launcher type");
    this.lockService = lockService;
    this.launcherType = launcherType;
  }

  @Override
  public void init(String launcherName) {
    Assert.notNull(launcherName, "Missing launcher name");
    this.launcherName = launcherName;
  }

  @Override
  public String getLauncherName() {
    return launcherName;
  }

  @Override
  public void lock() {
    Assert.notNull(launcherName, "Missing launcher name");
    log.atInfo().log("Locking launcher: " + launcherName);
    lock = lockService.lockLauncher(launcherName, launcherType);
    if (lock == null) {
      throw new RuntimeException("Could not lock launcher " + launcherName);
    }
  }

  @Override
  public void renewLock() {
    Assert.notNull(lock, "Missing lock");
    log.atInfo().log("Re-locking launcher: " + launcherName);
    if (!lockService.relockLauncher(lock)) {
      throw new RuntimeException("Could not re-lock launcher " + launcherName);
    }
  }

  @Override
  public void unlock() {
    Assert.notNull(lock, "Missing lock");
    log.atInfo().log("Unlocking launcher: " + launcherName);
    try {
      unlock(lockService, lock);
    } catch (Exception ex) {
      log.atSevere().log("Failed to unlock launcher: " + launcherName);
    }
  }

  public static void unlock(LockService lockService, LauncherLockEntity lock) {
    lockService.unlockProcesses(lock);
    lockService.unlockLauncher(lock);
  }

  @Override
  public boolean lockProcess(String pipelineName, String processId) {
    Assert.notNull(lock, "Missing lock");
    Assert.notNull(pipelineName, "Missing pipeline name");
    Assert.notNull(processId, "Missing process id");
    return lockService.lockProcess(lock, pipelineName, processId);
  }

  @Override
  public void unlockProcess(String pipelineName, String processId) {
    Assert.notNull(lock, "Missing lock");
    Assert.notNull(pipelineName, "Missing pipeline name");
    Assert.notNull(processId, "Missing process id");
    lockService.unlockProcess(lock, pipelineName, processId);
  }

  @Override
  public LauncherLockEntity getLock() {
    return lock;
  }
}
