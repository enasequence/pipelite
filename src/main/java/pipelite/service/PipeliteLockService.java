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
package pipelite.service;

import java.util.Optional;
import lombok.extern.flogger.Flogger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import pipelite.entity.PipeliteLock;
import pipelite.log.LogKey;
import pipelite.repository.PipeliteLockRepository;

// TODO: launcher lock is stored as a process lock and there is a possibility of lock name conflict

@Service
@Transactional(propagation = Propagation.REQUIRES_NEW)
@Flogger
public class PipeliteLockService {

  private final PipeliteLockRepository repository;

  public PipeliteLockService(@Autowired PipeliteLockRepository repository) {
    this.repository = repository;
  }

  public boolean lockLauncher(String launcherName, String processName) {
    try {
      return lock(getLauncherLock(launcherName, processName));
    } catch (Exception ex) {
      log.atSevere()
          .with(LogKey.LAUNCHER_NAME, launcherName)
          .with(LogKey.PROCESS_NAME, processName)
          .withCause(ex)
          .log("Failed to lock launcher");
      return false;
    }
  }

  public boolean isLauncherLocked(String launcherName, String processName) {
    return isLocked(processName, launcherName);
  }

  public boolean unlockLauncher(String launcherName, String processName) {
    try {
      return unlock(getLauncherLock(launcherName, processName));
    } catch (Exception ex) {
      log.atSevere()
          .with(LogKey.LAUNCHER_NAME, launcherName)
          .with(LogKey.PROCESS_NAME, processName)
          .withCause(ex)
          .log("Failed to unlock launcher");
      return false;
    }
  }

  public void purgeLauncherLocks(String launcherName, String processName) {
    repository.deleteByLauncherNameAndProcessName(launcherName, processName);
  }

  public boolean lockProcess(String launcherName, String processName, String processId) {
    try {
      return lock(getProcessLock(launcherName, processName, processId));
    } catch (Exception ex) {
      log.atSevere()
          .with(LogKey.LAUNCHER_NAME, launcherName)
          .with(LogKey.PROCESS_NAME, processName)
          .with(LogKey.PROCESS_ID, processId)
          .withCause(ex)
          .log("Failed to lock process launcher");
      return false;
    }
  }

  public boolean unlockProcess(String launcherName, String processName, String processId) {
    try {
      return unlock(getProcessLock(launcherName, processName, processId));
    } catch (Exception ex) {
      log.atSevere()
          .with(LogKey.LAUNCHER_NAME, launcherName)
          .with(LogKey.PROCESS_NAME, processName)
          .with(LogKey.PROCESS_ID, processId)
          .withCause(ex)
          .log("Failed to unlock process launcher");
      return false;
    }
  }

  public boolean isProcessLocked(String processName, String processId) {
    return isLocked(processName, processId);
  }

  public boolean isProcessLocked(String launcherName, String processName, String processId) {
    return isLocked(launcherName, processName, processId);
  }

  private static PipeliteLock getLauncherLock(String launcherName, String processName) {
    return new PipeliteLock(launcherName, processName, launcherName);
  }

  private static PipeliteLock getProcessLock(
      String launcherName, String processName, String processId) {
    return new PipeliteLock(launcherName, processName, processId);
  }

  private boolean lock(PipeliteLock pipeliteLock) {
    if (isLocked(pipeliteLock.getProcessName(), pipeliteLock.getLockId())) {
      return false;
    }
    repository.save(pipeliteLock);
    return true;
  }

  private boolean isLocked(String processName, String lockId) {
    return repository.findByProcessNameAndLockId(processName, lockId).isPresent();
  }

  private boolean isLocked(String launcherName, String processName, String lockId) {
    return repository
        .findByLauncherNameAndProcessNameAndLockId(launcherName, processName, lockId)
        .isPresent();
  }

  private boolean unlock(PipeliteLock pipeliteLock) {
    Optional<PipeliteLock> activeLock =
        repository.findByProcessNameAndLockId(
            pipeliteLock.getProcessName(), pipeliteLock.getLockId());
    if (activeLock.isPresent()) {
      if (!activeLock.get().getLauncherName().equals(pipeliteLock.getLauncherName())) {
        log.atSevere()
            .with(LogKey.LAUNCHER_NAME, pipeliteLock.getLauncherName())
            .with(LogKey.PROCESS_NAME, pipeliteLock.getProcessName())
            .with(LogKey.PROCESS_ID, pipeliteLock.getLockId())
            .log(
                "Failed to unlock lock. Lock held by different launcher %s",
                activeLock.get().getLauncherName());
        return false;
      }
      repository.delete(pipeliteLock);
      return true;
    }
    return false;
  }
}
