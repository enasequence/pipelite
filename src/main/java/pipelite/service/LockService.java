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
import java.util.Optional;
import lombok.extern.flogger.Flogger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.Assert;
import pipelite.configuration.AdvancedConfiguration;
import pipelite.configuration.ServiceConfiguration;
import pipelite.entity.ProcessLockEntity;
import pipelite.entity.ServiceLockEntity;
import pipelite.log.LogKey;
import pipelite.repository.ProcessLockRepository;
import pipelite.repository.ServiceLockRepository;

@Service
@Transactional(propagation = Propagation.REQUIRES_NEW)
@Flogger
public class LockService {

  private final ServiceConfiguration serviceConfiguration;
  private final ServiceLockRepository serviceLockRepository;
  private final ProcessLockRepository processLockRepository;
  private final Duration lockDuration;

  public LockService(
      @Autowired ServiceConfiguration serviceConfiguration,
      @Autowired AdvancedConfiguration advancedConfiguration,
      @Autowired ServiceLockRepository serviceLockRepository,
      @Autowired ProcessLockRepository processLockRepository) {
    this.serviceConfiguration = serviceConfiguration;
    this.serviceLockRepository = serviceLockRepository;
    this.processLockRepository = processLockRepository;
    this.lockDuration = advancedConfiguration.getLockDuration();
  }

  /**
   * Locks the launcher.
   *
   * @param lockService the lock service
   * @param serviceName the service name
   * @param lockId the unique service lock id
   * @return the service lock or null if the lock could not be created.
   */
  public static ServiceLockEntity lockService(
      LockService lockService, String serviceName, String lockId) {
    Assert.notNull(lockService, "Missing lock service");
    // Catch DataIntegrityViolationException that my be thrown during commit.
    try {
      return lockService.lockServiceThrowsDataIntegrityViolationException(serviceName, lockId);
    } catch (DataIntegrityViolationException ex) {
      // Locking the service may throw DataIntegrityViolationException during commit.
      log.atSevere().with(LogKey.SERVICE_NAME, serviceName).log("Failed to lock service");
      return null;
    }
  }

  public ServiceLockEntity lockServiceThrowsDataIntegrityViolationException(
      String serviceName, String lockId) {
    log.atFine().with(LogKey.SERVICE_NAME, serviceName).log("Attempting to lock service");
    removeExpiredServiceLock(serviceName);
    ServiceLockEntity serviceLock = new ServiceLockEntity();
    serviceLock.setLockId(lockId);
    serviceLock.setServiceName(serviceName);
    serviceLock.setExpiry(ZonedDateTime.now().plus(lockDuration));
    serviceLock.setHost(ServiceConfiguration.getCanonicalHostName());
    serviceLock.setPort(serviceConfiguration.getPort());
    serviceLock.setContextPath(serviceConfiguration.getContextPath());
    serviceLock = serviceLockRepository.save(serviceLock);
    log.atFine().with(LogKey.SERVICE_NAME, serviceName).log("Locked service");
    return serviceLock;
  }

  /**
   * Remove expired service lock.
   *
   * @param serviceName the service name
   * @return true if an expired service lock was removed
   */
  private boolean removeExpiredServiceLock(String serviceName) {
    if (serviceLockRepository.deleteByServiceNameAndExpiryLessThanEqual(
            serviceName, ZonedDateTime.now())
        > 0) {
      // Flush to make sure delete is done before new lock is saved.
      serviceLockRepository.flush();
      log.atInfo().with(LogKey.SERVICE_NAME, serviceName).log("Removed expired service lock");
      return true;
    }
    return false;
  }

  /**
   * Relocks the launcher to avoid lock expiry.
   *
   * @param serviceLock the service lock
   * @return true if successful.
   */
  public boolean relockService(ServiceLockEntity serviceLock) {
    String serviceName = serviceLock.getServiceName();
    log.atFine().with(LogKey.SERVICE_NAME, serviceName).log("Attempting to relock service");
    try {
      serviceLock.setExpiry(ZonedDateTime.now().plus(lockDuration));
      serviceLockRepository.save(serviceLock);
      log.atFine().with(LogKey.SERVICE_NAME, serviceName).log("Relocked service");
      return true;
    } catch (Exception ex) {
      log.atSevere()
          .with(LogKey.SERVICE_NAME, serviceName)
          .withCause(ex)
          .log("Failed to relock service");
      return false;
    }
  }

  /**
   * Unlocks the service.
   *
   * @param serviceName the service name
   */
  public void unlockService(String serviceName) {
    log.atFine().with(LogKey.SERVICE_NAME, serviceName).log("Attempting to unlock service");
    try {
      serviceLockRepository.deleteByServiceName(serviceName);
      log.atFine().with(LogKey.SERVICE_NAME, serviceName).log("Unlocked service");
    } catch (Exception ex) {
      log.atSevere()
          .with(LogKey.SERVICE_NAME, serviceName)
          .withCause(ex)
          .log("Failed to unlock service");
    }
  }

  /**
   * Locks the process.
   *
   * @param lockService the lock service
   * @param serviceLock the service lock
   * @param pipelineName the pipeline name
   * @param processId the process id
   * @return true if successful.
   */
  public static boolean lockProcess(
      LockService lockService,
      ServiceLockEntity serviceLock,
      String pipelineName,
      String processId) {
    Assert.notNull(lockService, "Missing lock service");
    String serviceName = serviceLock.getServiceName();
    // Catch DataIntegrityViolationException that my be thrown during commit.
    try {
      return lockService.lockProcessThrowsDataIntegrityViolationException(
          serviceLock, pipelineName, processId);
    } catch (DataIntegrityViolationException ex) {
      // Locking a process may throw DataIntegrityViolationException during commit.
      log.atSevere()
          .with(LogKey.SERVICE_NAME, serviceName)
          .with(LogKey.PIPELINE_NAME, pipelineName)
          .with(LogKey.PROCESS_ID, processId)
          // .withCause(ex)
          .log("Failed to lock process");
      return false;
    }
  }

  public boolean lockProcessThrowsDataIntegrityViolationException(
      ServiceLockEntity serviceLock, String pipelineName, String processId) {
    String serviceName = serviceLock.getServiceName();
    log.atFine()
        .with(LogKey.SERVICE_NAME, serviceName)
        .with(LogKey.PIPELINE_NAME, pipelineName)
        .with(LogKey.PROCESS_ID, processId)
        .log("Attempting to lock process:");

    Optional<ProcessLockEntity> processLock =
        processLockRepository.findByPipelineNameAndProcessId(pipelineName, processId);

    if (!processLock.isPresent()) {
      // The process is not locked.
      // Create process lock.
      lockProcess(pipelineName, processId, serviceName);
      return true;
    }

    if (processLock.get().getServiceName().equals(serviceName)) {
      // The process is already locked by the same service.
      return true;
    }

    Optional<ServiceLockEntity> processServiceLock =
        serviceLockRepository.findByServiceName(processLock.get().getServiceName());

    if (!processServiceLock.isPresent()
        || !processServiceLock.get().getExpiry().isAfter(ZonedDateTime.now())) {
      // Remove process lock as service lock does not exist.
      // Remove process lock as service lock has expired.
      // Create process lock.
      processLockRepository.deleteByPipelineNameAndProcessId(pipelineName, processId);
      lockProcess(pipelineName, processId, serviceName);
      return true;
    }

    return false;
  }

  private void lockProcess(String pipelineName, String processId, String serviceName) {
    ProcessLockEntity processLock = new ProcessLockEntity();
    processLock.setServiceName(serviceName);
    processLock.setPipelineName(pipelineName);
    processLock.setProcessId(processId);
    processLockRepository.save(processLock);
    log.atFine()
        .with(LogKey.SERVICE_NAME, serviceName)
        .with(LogKey.PIPELINE_NAME, pipelineName)
        .with(LogKey.PROCESS_ID, processId)
        .log("Locked process");
  }

  /**
   * Unlocks the process.
   *
   * @param serviceLock the service lock
   * @param pipelineName the pipeline name
   * @param processId the process id
   * @return true if successful.
   */
  public boolean unlockProcess(
      ServiceLockEntity serviceLock, String pipelineName, String processId) {
    String serviceName = serviceLock.getServiceName();
    log.atFine()
        .with(LogKey.SERVICE_NAME, serviceName)
        .with(LogKey.PIPELINE_NAME, pipelineName)
        .with(LogKey.PROCESS_ID, processId)
        .log("Attempting to unlock process");
    try {
      ProcessLockEntity processLock = new ProcessLockEntity();
      processLock.setServiceName(serviceName);
      processLock.setPipelineName(pipelineName);
      processLock.setProcessId(processId);
      processLockRepository.delete(processLock);
      log.atFine()
          .with(LogKey.SERVICE_NAME, serviceName)
          .with(LogKey.PIPELINE_NAME, pipelineName)
          .with(LogKey.PROCESS_ID, processId)
          .log("Unlocked process");
      return true;
    } catch (Exception ex) {
      log.atSevere()
          .with(LogKey.SERVICE_NAME, serviceName)
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
   * @param serviceName the service name
   */
  public void unlockProcesses(String serviceName) {
    log.atFine().with(LogKey.SERVICE_NAME, serviceName).log("Attempting to unlock processes");
    try {
      processLockRepository.deleteByServiceName(serviceName);
      log.atFine().with(LogKey.SERVICE_NAME, serviceName).log("Unlocked processes");
    } catch (Exception ex) {
      log.atSevere()
          .with(LogKey.SERVICE_NAME, serviceName)
          .withCause(ex)
          .log("Failed to unlock processes");
    }
  }

  /**
   * Returns true if the service is locked.
   *
   * @param serviceName the service name
   * @return true if the launcher is locked
   */
  public boolean isServiceLocked(String serviceName) {
    Optional<ServiceLockEntity> serviceLock = serviceLockRepository.findByServiceName(serviceName);
    if (!serviceLock.isPresent()) {
      return false;
    }
    return serviceLock.get().getExpiry().isAfter(ZonedDateTime.now());
  }

  /**
   * Returns true if the process is locked and the lock has not expired.
   *
   * @param pipelineName the pipeline name
   * @param processId the process id
   * @return true if the process is locked
   */
  public boolean isProcessLocked(String pipelineName, String processId) {
    Optional<ProcessLockEntity> processLock =
        processLockRepository.findByPipelineNameAndProcessId(pipelineName, processId);
    if (!processLock.isPresent()) {
      // There is no process lock.
      return false;
    }
    Optional<ServiceLockEntity> serviceLock =
        serviceLockRepository.findByServiceName(processLock.get().getServiceName());
    if (!serviceLock.isPresent() || !serviceLock.get().getExpiry().isAfter(ZonedDateTime.now())) {
      // The service lock does not exist.
      // The service lock has expired.
      return false;
    }
    return true;
  }

  /**
   * Returns the lock duration.
   *
   * @return the lock duration.
   */
  public Duration getLockDuration() {
    return lockDuration;
  }
}
