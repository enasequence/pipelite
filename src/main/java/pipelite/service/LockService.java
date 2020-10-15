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
import pipelite.entity.LockEntity;
import pipelite.log.LogKey;
import pipelite.repository.LockRepository;

// TODO: launcher lock is stored as a process lock and there is a possibility of lock name conflict

@Service
@Transactional(propagation = Propagation.REQUIRES_NEW)
@Flogger
public class LockService {

  private final LockRepository repository;

  public LockService(@Autowired LockRepository repository) {
    this.repository = repository;
  }

  public boolean lockLauncher(String launcherName, String pipelineName) {
    try {
      return lock(getLauncherLock(launcherName, pipelineName));
    } catch (Exception ex) {
      log.atSevere()
          .with(LogKey.LAUNCHER_NAME, launcherName)
          .with(LogKey.PIPELINE_NAME, pipelineName)
          .withCause(ex)
          .log("Failed to lock launcher");
      return false;
    }
  }

  public boolean isLauncherLocked(String launcherName, String pipelineName) {
    return isLocked(pipelineName, launcherName);
  }

  public boolean unlockLauncher(String launcherName, String pipelineName) {
    try {
      return unlock(getLauncherLock(launcherName, pipelineName));
    } catch (Exception ex) {
      log.atSevere()
          .with(LogKey.LAUNCHER_NAME, launcherName)
          .with(LogKey.PIPELINE_NAME, pipelineName)
          .withCause(ex)
          .log("Failed to unlock launcher");
      return false;
    }
  }

  public boolean removeLauncherLocks(String launcherName, String pipelineName) {
    try {
    repository.deleteByLauncherNameAndPipelineName(launcherName, pipelineName);
    return true;
    } catch (Exception ex) {
      log.atSevere()
              .with(LogKey.LAUNCHER_NAME, launcherName)
              .with(LogKey.PIPELINE_NAME, pipelineName)
              .withCause(ex)
              .log("Failed to remove launcher locks");
      return false;
    }
  }

  public boolean removeLauncherLocks(String launcherName) {
    try {
      repository.deleteByLauncherName(launcherName);
      return true;
    } catch (Exception ex) {
      log.atSevere()
              .with(LogKey.LAUNCHER_NAME, launcherName)
              .withCause(ex)
              .log("Failed to remove launcher locks");
      return false;
    }
  }

  public boolean lockProcess(String launcherName, String pipelineName, String processId) {
    try {
      return lock(getProcessLock(launcherName, pipelineName, processId));
    } catch (Exception ex) {
      log.atSevere()
          .with(LogKey.LAUNCHER_NAME, launcherName)
          .with(LogKey.PIPELINE_NAME, pipelineName)
          .with(LogKey.PROCESS_ID, processId)
          .withCause(ex)
          .log("Failed to lock process launcher");
      return false;
    }
  }

  public boolean unlockProcess(String launcherName, String pipelineName, String processId) {
    try {
      return unlock(getProcessLock(launcherName, pipelineName, processId));
    } catch (Exception ex) {
      log.atSevere()
          .with(LogKey.LAUNCHER_NAME, launcherName)
          .with(LogKey.PIPELINE_NAME, pipelineName)
          .with(LogKey.PROCESS_ID, processId)
          .withCause(ex)
          .log("Failed to unlock process launcher");
      return false;
    }
  }

  public boolean isProcessLocked(String pipelineName, String processId) {
    return isLocked(pipelineName, processId);
  }

  public boolean isProcessLocked(String launcherName, String pipelineName, String processId) {
    return isLocked(launcherName, pipelineName, processId);
  }

  private static LockEntity getLauncherLock(String launcherName, String pipelineName) {
    return new LockEntity(launcherName, pipelineName, launcherName);
  }

  private static LockEntity getProcessLock(
      String launcherName, String pipelineName, String processId) {
    return new LockEntity(launcherName, pipelineName, processId);
  }

  private boolean lock(LockEntity lockEntity) {
    if (isLocked(lockEntity.getPipelineName(), lockEntity.getLockId())) {
      return false;
    }
    repository.save(lockEntity);
    return true;
  }

  private boolean isLocked(String pipelineName, String lockId) {
    return repository.findByPipelineNameAndLockId(pipelineName, lockId).isPresent();
  }

  private boolean isLocked(String launcherName, String pipelineName, String lockId) {
    return repository
        .findByLauncherNameAndPipelineNameAndLockId(launcherName, pipelineName, lockId)
        .isPresent();
  }

  private boolean unlock(LockEntity lockEntity) {
    Optional<LockEntity> activeLock =
        repository.findByPipelineNameAndLockId(
            lockEntity.getPipelineName(), lockEntity.getLockId());
    if (activeLock.isPresent()) {
      if (!activeLock.get().getLauncherName().equals(lockEntity.getLauncherName())) {
        log.atSevere()
            .with(LogKey.LAUNCHER_NAME, lockEntity.getLauncherName())
            .with(LogKey.PIPELINE_NAME, lockEntity.getPipelineName())
            .with(LogKey.PROCESS_ID, lockEntity.getLockId())
            .log(
                "Failed to unlock lock. Lock held by different launcher %s",
                activeLock.get().getLauncherName());
        return false;
      }
      repository.delete(lockEntity);
      return true;
    }
    return false;
  }
}
