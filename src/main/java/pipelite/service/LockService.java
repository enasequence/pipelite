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
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import lombok.extern.flogger.Flogger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionTemplate;
import pipelite.configuration.LauncherConfiguration;
import pipelite.configuration.WebConfiguration;
import pipelite.entity.LauncherLockEntity;
import pipelite.entity.ProcessLockEntity;
import pipelite.launcher.process.runner.ProcessRunnerType;
import pipelite.log.LogKey;
import pipelite.repository.LauncherLockRepository;
import pipelite.repository.ProcessLockRepository;

@Service
@Transactional(propagation = Propagation.REQUIRES_NEW)
@Flogger
public class LockService {

  private final LauncherConfiguration launcherConfiguration;
  private final WebConfiguration webConfiguration;
  private final LauncherLockRepository launcherLockRepository;
  private final ProcessLockRepository processLockRepository;
  private final PlatformTransactionManager transactionManager;
  private Duration lockDuration;

  public LockService(
      @Autowired LauncherConfiguration launcherConfiguration,
      @Autowired WebConfiguration webConfiguration,
      @Autowired LauncherLockRepository launcherLockRepository,
      @Autowired ProcessLockRepository processLockRepository,
      @Autowired PlatformTransactionManager transactionManager) {
    this.launcherConfiguration = launcherConfiguration;
    this.webConfiguration = webConfiguration;
    this.launcherLockRepository = launcherLockRepository;
    this.processLockRepository = processLockRepository;
    this.transactionManager = transactionManager;
    this.lockDuration = launcherConfiguration.getPipelineLockDuration();
  }

  /**
   * Locks the launcher.
   *
   * @param launcherName the launcher name
   * @param launcherType the launcher type
   * @return the launcher lock or null if the lock could not be created.
   */
  public LauncherLockEntity lockLauncher(String launcherName, ProcessRunnerType launcherType) {
    // Use transaction template to catch DataIntegrityViolationException thrown during commit.
    TransactionTemplate transactionTemplate = new TransactionTemplate(transactionManager);
    transactionTemplate.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
    try {
      return transactionTemplate.execute(status -> lockLauncherThrows(launcherName, launcherType));
    } catch (DataIntegrityViolationException ex) {
      // Locking a launcher may throw DataIntegrityViolationException during commit.
      log.atSevere()
          .with(LogKey.LAUNCHER_NAME, launcherName)
          // .withCause(ex)
          .log("Failed to lock launcher");
      return null;
    }
  }

  private LauncherLockEntity lockLauncherThrows(
      String launcherName, ProcessRunnerType launcherType) {
    log.atInfo().with(LogKey.LAUNCHER_NAME, launcherName).log("Attempting to lock launcher");
    removeExpiredLauncherLock(launcherName);
    LauncherLockEntity launcherLock = new LauncherLockEntity();
    launcherLock.setLauncherName(launcherName);
    launcherLock.setLauncherType(launcherType);
    launcherLock.setExpiry(ZonedDateTime.now().plus(lockDuration));
    launcherLock.setHost(WebConfiguration.getCanonicalHostName());
    launcherLock.setPort(webConfiguration.getPort());
    launcherLock.setContextPath(webConfiguration.getContextPath());
    launcherLock = launcherLockRepository.save(launcherLock);
    if (launcherLock.getLauncherId() == null) {
      log.atSevere()
          .with(LogKey.LAUNCHER_NAME, launcherName)
          .log("Failed to lock launcher: missing launcher id");
      return null;
    }
    log.atInfo().with(LogKey.LAUNCHER_NAME, launcherName).log("Locked launcher");
    return launcherLock;
  }

  /**
   * Remove expired launcher lock.
   *
   * @param launcherName the launcher name
   * @return true if an expired launcher lock was removed
   */
  private boolean removeExpiredLauncherLock(String launcherName) {
    if (launcherLockRepository.deleteByLauncherNameAndExpiryLessThanEqual(
            launcherName, ZonedDateTime.now())
        > 0) {
      // Flush to make sure delete is done before new lock is saved.
      launcherLockRepository.flush();
      log.atInfo().with(LogKey.LAUNCHER_NAME, launcherName).log("Removed expired launcher lock");
      return true;
    }
    return false;
  }

  /**
   * Relocks the launcher to avoid lock expiry.
   *
   * @param launcherLock the launcher lock
   * @return true if successful.
   */
  public boolean relockLauncher(LauncherLockEntity launcherLock) {
    log.atInfo()
        .with(LogKey.LAUNCHER_NAME, launcherLock.getLauncherName())
        .log("Attempting to relock launcher");
    try {
      launcherLock.setExpiry(ZonedDateTime.now().plus(lockDuration));
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

  /**
   * Unlocks the launcher.
   *
   * @param launcherLock the launcher lock
   */
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
   * @param launcherLock the launcher lock
   * @param pipelineName the pipeline name
   * @param processId the process id
   * @return true if successful.
   */
  public boolean lockProcess(
      LauncherLockEntity launcherLock, String pipelineName, String processId) {
    // Use transaction template to catch DataIntegrityViolationException thrown during commit.
    TransactionTemplate transactionTemplate = new TransactionTemplate(transactionManager);
    transactionTemplate.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
    try {
      return transactionTemplate.execute(
          status -> lockProcessThrows(launcherLock, pipelineName, processId));
    } catch (DataIntegrityViolationException ex) {
      // Locking a process may throw DataIntegrityViolationException during commit.
      log.atSevere()
          .with(LogKey.LAUNCHER_NAME, launcherLock.getLauncherName())
          .with(LogKey.PIPELINE_NAME, pipelineName)
          .with(LogKey.PROCESS_ID, processId)
          // .withCause(ex)
          .log("Failed to lock process");
      return false;
    }
  }

  private boolean lockProcessThrows(
      LauncherLockEntity launcherLock, String pipelineName, String processId) {
    log.atInfo()
        .with(LogKey.LAUNCHER_NAME, launcherLock.getLauncherName())
        .with(LogKey.PIPELINE_NAME, pipelineName)
        .with(LogKey.PROCESS_ID, processId)
        .log("Attempting to lock process");
    removeExpiredProcessLock(pipelineName, processId);

    if (isProcessLocked(pipelineName, processId)) {
      return false;
    }

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
  }

  /**
   * Remove expired process lock.
   *
   * @param pipelineName the pipeline name
   * @param processId the process id
   * @return true if an expired process lock was removed
   */
  private boolean removeExpiredProcessLock(String pipelineName, String processId) {
    Optional<ProcessLockEntity> processLock =
        processLockRepository.findByPipelineNameAndProcessId(pipelineName, processId);
    if (!processLock.isPresent()) {
      return false;
    }
    Optional<LauncherLockEntity> launcherLock =
        launcherLockRepository.findById(processLock.get().getLauncherId());
    if (!launcherLock.isPresent()) {
      // Remove process lock as launcher lock does not exist.
      return processLockRepository.deleteByPipelineNameAndProcessId(pipelineName, processId) > 0;
    }
    if (!launcherLock.get().getExpiry().isAfter(ZonedDateTime.now())) {
      // Remove process lock as launcher lock has expired.
      return processLockRepository.deleteByPipelineNameAndProcessId(pipelineName, processId) > 0;
    }
    return false;
  }

  /**
   * Unlocks the process.
   *
   * @param launcherLock the launcher lock
   * @param pipelineName the pipeline name
   * @param processId the process id
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

  /**
   * Unlocks the processes associated with a launcher.
   *
   * @param launcherLock the launcher lock
   */
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

  /**
   * Returns true if the launcher is locked.
   *
   * @param launcherName the launcher name
   * @return true if the launcher is locked
   */
  public boolean isLauncherLocked(String launcherName) {
    Optional<LauncherLockEntity> launcherLock =
        launcherLockRepository.findByLauncherName(launcherName);
    if (!launcherLock.isPresent()) {
      return false;
    }
    return launcherLock.get().getExpiry().isAfter(ZonedDateTime.now());
  }

  /**
   * Returns true if the process is locked.
   *
   * @param pipelineName the pipeline name
   * @param processId the process id
   * @return true if the process is locked
   */
  public boolean isProcessLocked(String pipelineName, String processId) {
    Optional<ProcessLockEntity> processLock =
        processLockRepository.findByPipelineNameAndProcessId(pipelineName, processId);
    if (!processLock.isPresent()) {
      return false;
    }
    Optional<LauncherLockEntity> launcherLock =
        launcherLockRepository.findById(processLock.get().getLauncherId());
    if (!launcherLock.isPresent()) {
      return false;
    }
    return launcherLock.get().getExpiry().isAfter(ZonedDateTime.now());
  }

  /**
   * Returns the lock duration.
   *
   * @return the lock duration.
   */
  public Duration getLockDuration() {
    return lockDuration;
  }

  public List<LauncherLockEntity> getLauncherLocksByLauncherName(String launcherName) {
    Optional<LauncherLockEntity> lock = launcherLockRepository.findByLauncherName(launcherName);
    if (lock.isPresent()) {
      return Arrays.asList(lock.get());
    }
    return Collections.emptyList();
  }
}
