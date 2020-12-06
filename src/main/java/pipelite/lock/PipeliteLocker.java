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

import pipelite.entity.LauncherLockEntity;

/**
 * Manages launcher locks. Launcher locks prevent two launchers with the same name from being
 * executed at the same time. Launcher lock may expire. If it does then the launcher lock and any
 * associated process locks will be removed by the PipeliteUnlocker. Lock expiry can be prevented by
 * renewing the launcher lock.
 */
public interface PipeliteLocker {

  /**
   * Initialises the locker. Must be called before calling any other methods.
   *
   * @param launcherName the unique launcher name
   */
  void init(String launcherName);

  /**
   * Returns the launcher name.
   *
   * @return the launcher name
   */
  String getLauncherName();

  /**
   * Locks the launcher.
   *
   * @throws RuntimeException if the launcher could not be locked
   */
  void lock();

  /**
   * Renews the launcher lock.
   *
   * @throws RuntimeException if the launcher lock could not be renewed
   */
  void renewLock();

  /** Removes locks associated with the launcher. */
  void unlock();

  /**
   * Locks a process associated with this launcher.
   *
   * @return false if the process could not be locked
   */
  boolean lockProcess(String pipelineName, String processId);

  /**
   * Unlocks a process associated with this launcher.
   *
   * @param pipelineName the pipeline name
   * @param processId the process id
   */
  void unlockProcess(String pipelineName, String processId);

  /**
   * Returns the lock.
   *
   * @return the lock
   */
  LauncherLockEntity getLock();
}
