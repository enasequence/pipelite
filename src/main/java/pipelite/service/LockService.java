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

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import lombok.extern.flogger.Flogger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import pipelite.configuration.LauncherConfiguration;
import pipelite.entity.LauncherLockEntity;
import pipelite.entity.ProcessLockEntity;
import pipelite.log.LogKey;
import pipelite.repository.LauncherLockRepository;
import pipelite.repository.ProcessLockRepository;

@Service
@Transactional(propagation = Propagation.REQUIRES_NEW)
@Flogger
public class LockService {

  private final LauncherConfiguration launcherConfiguration;
  private final LauncherLockRepository launcherLockRepository;
  private final ProcessLockRepository processLockRepository;
  private final Duration lockDuration;

  public LockService(
      @Autowired LauncherConfiguration launcherConfiguration,
      @Autowired LauncherLockRepository launcherLockRepository,
      @Autowired ProcessLockRepository processLockRepository) {
    this.launcherConfiguration = launcherConfiguration;
    this.launcherLockRepository = launcherLockRepository;
    this.processLockRepository = processLockRepository;
    this.lockDuration = launcherConfiguration.getPipelineLockDuration();
  }

  /**
   * Locks the launcher.
   *
   * @return the launcher lock or null if the lock could not be created.
   */
  public LauncherLockEntity lockLauncher(String launcherName) {
    log.atInfo().with(LogKey.LAUNCHER_NAME, launcherName).log("Attempting to lock launcher");
    try {
      LauncherLockEntity launcherLock = new LauncherLockEntity();
      launcherLock.setLauncherName(launcherName);
      launcherLock.setExpiry(LocalDateTime.now().plus(lockDuration));
      launcherLock = launcherLockRepository.save(launcherLock);
      if (launcherLock.getLauncherId() == null) {
        log.atSevere()
            .with(LogKey.LAUNCHER_NAME, launcherName)
            .log("Failed to lock launcher: missing launcher id");
        return null;
      }
      log.atInfo().with(LogKey.LAUNCHER_NAME, launcherName).log("Locked launcher");
      return launcherLock;
    } catch (Exception ex) {
      log.atSevere()
          .with(LogKey.LAUNCHER_NAME, launcherName)
          .withCause(ex)
          .log("Failed to lock launcher");
      return null;
    }
  }

  /**
   * Relocks the launcher to avoid lock expiry.
   *
   * @return true if successful.
   */
  public boolean relockLauncher(LauncherLockEntity launcherLock) {
    log.atInfo()
        .with(LogKey.LAUNCHER_NAME, launcherLock.getLauncherName())
        .log("Attempting to relock launcher");
    try {
      launcherLock.setExpiry(LocalDateTime.now().plus(lockDuration));
      launcherLockRepository.save(launcherLock);
      log.atInfo()
          .with(LogKey.LAUNCHER_NAME, launcherLock.getLauncherName())
          .log("Relock launcher");
      return true;
    } catch (Exception ex) {
      log.atSevere()
          .with(LogKey.LAUNCHER_NAME, launcherLock.getLauncherName())
          .withCause(ex)
          .log("Failed to relock launcher");
      return false;
    }
  }

  /** Unlocks the launcher. */
  public void unlockLauncher(LauncherLockEntity launcherLock) {
    log.atInfo()
        .with(LogKey.LAUNCHER_NAME, launcherLock.getLauncherName())
        .log("Attempting to unlock launcher");
    try {
      launcherLockRepository.delete(launcherLock);
      log.atInfo()
          .with(LogKey.LAUNCHER_NAME, launcherLock.getLauncherName())
          .log("Unlocked launcher");
    } catch (Exception ex) {
      log.atSevere()
          .with(LogKey.LAUNCHER_NAME, launcherLock.getLauncherName())
          .withCause(ex)
          .log("Failed to unlock launcher");
    }
  }

  /**
   * Locks the process.
   *
   * @return true if successful.
   */
  public boolean lockProcess(
      LauncherLockEntity launcherLock, String pipelineName, String processId) {
    log.atInfo()
        .with(LogKey.LAUNCHER_NAME, launcherLock.getLauncherName())
        .with(LogKey.PIPELINE_NAME, pipelineName)
        .with(LogKey.PROCESS_ID, processId)
        .log("Attempting to lock process");
    try {
      ProcessLockEntity processLock = new ProcessLockEntity();
      processLock.setLauncherId(launcherLock.getLauncherId());
      processLock.setPipelineName(pipelineName);
      processLock.setProcessId(processId);
      processLockRepository.save(processLock);
      log.atInfo()
          .with(LogKey.LAUNCHER_NAME, launcherLock.getLauncherName())
          .with(LogKey.PIPELINE_NAME, pipelineName)
          .with(LogKey.PROCESS_ID, processId)
          .log("Locked process");
      return true;
    } catch (Exception ex) {
      log.atSevere()
          .with(LogKey.LAUNCHER_NAME, launcherLock.getLauncherName())
          .with(LogKey.PIPELINE_NAME, pipelineName)
          .with(LogKey.PROCESS_ID, processId)
          .withCause(ex)
          .log("Failed to lock process");
      return false;
    }
  }

  /**
   * Unlocks the process.
   *
   * @return true if successful.
   */
  public boolean unlockProcess(
      LauncherLockEntity launcherLock, String pipelineName, String processId) {
    log.atInfo()
        .with(LogKey.LAUNCHER_NAME, launcherLock.getLauncherName())
        .with(LogKey.PIPELINE_NAME, pipelineName)
        .with(LogKey.PROCESS_ID, processId)
        .log("Attempting to unlock process");
    try {
      ProcessLockEntity processLock = new ProcessLockEntity();
      processLock.setLauncherId(launcherLock.getLauncherId());
      processLock.setPipelineName(pipelineName);
      processLock.setProcessId(processId);
      processLockRepository.delete(processLock);
      log.atInfo()
          .with(LogKey.LAUNCHER_NAME, launcherLock.getLauncherName())
          .with(LogKey.PIPELINE_NAME, pipelineName)
          .with(LogKey.PROCESS_ID, processId)
          .log("Unlocked process");
      return true;
    } catch (Exception ex) {
      log.atSevere()
          .with(LogKey.LAUNCHER_NAME, launcherLock.getLauncherName())
          .with(LogKey.PIPELINE_NAME, pipelineName)
          .with(LogKey.PROCESS_ID, processId)
          .withCause(ex)
          .log("Failed to unlock process");
      return false;
    }
  }

  /** Unlocks the processes associated with a pipeline. */
  public void unlockProcessesByPipelineName(String pipelineName) {
    log.atInfo().with(LogKey.PIPELINE_NAME, pipelineName).log("Attempting to unlock processes");
    try {
      processLockRepository.deleteByPipelineName(pipelineName);
      log.atInfo().with(LogKey.PIPELINE_NAME, pipelineName).log("Unlock processes");
    } catch (Exception ex) {
      log.atSevere()
          .with(LogKey.PIPELINE_NAME, pipelineName)
          .withCause(ex)
          .log("Failed to unlock processes");
    }
  }

  /** Unlocks the processes associated with a launcher. */
  public void unlockProcesses(LauncherLockEntity launcherLock) {
    log.atInfo()
        .with(LogKey.LAUNCHER_NAME, launcherLock.getLauncherName())
        .log("Attempting to unlock processes");
    try {
      processLockRepository.deleteByLauncherId(launcherLock.getLauncherId());
      log.atInfo()
          .with(LogKey.LAUNCHER_NAME, launcherLock.getLauncherName())
          .log("Unlock processes");
    } catch (Exception ex) {
      log.atSevere()
          .with(LogKey.LAUNCHER_NAME, launcherLock.getLauncherName())
          .withCause(ex)
          .log("Failed to unlock processes");
    }
  }

  /** Checks if the process is locked. */
  public boolean isProcessLocked(String pipelineName, String processId) {
    return processLockRepository
        .findByPipelineNameAndProcessId(pipelineName, processId)
        .isPresent();
  }

  public List<LauncherLockEntity> getLauncherLocksByLauncherName(String launcherName) {
    return launcherLockRepository.findByLauncherName(launcherName);
  }

  public List<LauncherLockEntity> getExpiredLauncherLocks() {
    return launcherLockRepository.findByExpiryLessThan(LocalDateTime.now());
  }

  public List<ProcessLockEntity> getProcessLocks(LauncherLockEntity launcherLock) {
    return processLockRepository.findByLauncherId(launcherLock.getLauncherId());
  }
}
