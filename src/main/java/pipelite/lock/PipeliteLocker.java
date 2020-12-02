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
import pipelite.entity.LauncherLockEntity;
import pipelite.service.LockService;

@Flogger
/**
 * Manages launcher locks. Launcher locks prevent two launchers with the same name from being
 * executed at the same time. Launcher lock may expire. If it does then the launcher lock and any
 * associated process locks will be removed by the PipeliteUnlocker. Lock expiry can be prevented by
 * renewing the launcher lock.
 */
public class PipeliteLocker {

  private final LockService lockService;
  private final String launcherName;
  private LauncherLockEntity lock;

  public PipeliteLocker(LockService lockService, String launcherName) {
    this.lockService = lockService;
    this.launcherName = launcherName;
  }

  /**
   * Locks the launcher.
   *
   * @throws RuntimeException if the launcher could not be locked
   */
  public void lock() {
    log.atInfo().log("Locking launcher: " + launcherName);
    lock = lockService.lockLauncher(launcherName);
    if (lock == null) {
      throw new RuntimeException("Could not lock launcher " + launcherName);
    }
  }

  /**
   * Renews the launcher lock.
   *
   * @throws RuntimeException if the launcher lock could not be renewed
   */
  public void renewLock() {
    log.atInfo().log("Re-locking launcher: " + launcherName);
    if (!lockService.relockLauncher(lock)) {
      throw new RuntimeException("Could not re-lock launcher " + launcherName);
    }
  }

  /** Removes locks associated with the launcher. */
  public void unlock() {
    log.atInfo().log("Unlocking launcher: " + launcherName);
    try {
      unlock(lockService, lock);
    } catch (Exception ex) {
      log.atSevere().log("Failed to unlock launcher: " + launcherName);
    }
  }

  /** Removes locks associated with the launcher. */
  public static void unlock(LockService lockService, LauncherLockEntity lock) {
    lockService.unlockProcesses(lock);
    lockService.unlockLauncher(lock);
  }

  /**
   * Locks a process associated with this launcher.
   *
   * @return false if the process could not be locked
   */
  public boolean lockProcess(String pipelineName, String processId) {
    return lockService.lockProcess(lock, pipelineName, processId);
  }

  /**
   * Unlocks a process associated with this launcher.
   *
   * @param pipelineName the pipeline name
   * @param processId the process id
   */
  public void unlockProcess(String pipelineName, String processId) {
    lockService.unlockProcess(lock, pipelineName, processId);
  }

  public String getLauncherName() {
    return launcherName;
  }

  LauncherLockEntity getLock() {
    return lock;
  }
}
