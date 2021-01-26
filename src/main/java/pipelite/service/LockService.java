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
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.Assert;
import pipelite.configuration.AdvancedConfiguration;
import pipelite.configuration.ServiceConfiguration;
import pipelite.entity.ServiceLockEntity;
import pipelite.entity.ProcessLockEntity;
import pipelite.log.LogKey;
import pipelite.repository.ServiceLockRepository;
import pipelite.repository.ProcessLockRepository;

@Service
@Transactional(propagation = Propagation.REQUIRES_NEW)
@Flogger
public class LockService {

  private final ServiceConfiguration serviceConfiguration;
  private final ServiceLockRepository serviceLockRepository;
  private final ProcessLockRepository processLockRepository;
  private Duration lockDuration;

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
   * @return the service lock or null if the lock could not be created.
   */
  public static ServiceLockEntity lockService(LockService lockService, String serviceName) {
    Assert.notNull(lockService, "Missing lock service");
    // Catch DataIntegrityViolationException that my be thrown during commit.
    try {
      return lockService.lockServiceThrowsDataIntegrityViolationException(serviceName);
    } catch (DataIntegrityViolationException ex) {
      // Locking a launcher may throw DataIntegrityViolationException during commit.
      log.atSevere().with(LogKey.SERVICE_NAME, serviceName).log("Failed to lock service");
      return null;
    }
  }

  public ServiceLockEntity lockServiceThrowsDataIntegrityViolationException(String serviceName) {
    log.atFine().with(LogKey.SERVICE_NAME, serviceName).log("Attempting to lock service");
    removeExpiredServiceLock(serviceName);
    ServiceLockEntity serviceLock = new ServiceLockEntity();
    serviceLock.setServiceName(serviceName);
    serviceLock.setExpiry(ZonedDateTime.now().plus(lockDuration));
    serviceLock.setHost(ServiceConfiguration.getCanonicalHostName());
    serviceLock.setPort(serviceConfiguration.getPort());
    serviceLock.setContextPath(serviceConfiguration.getContextPath());
    serviceLock = serviceLockRepository.save(serviceLock);
    if (serviceLock.getServiceId() == null) {
      log.atSevere()
          .with(LogKey.SERVICE_NAME, serviceName)
          .log("Failed to lock service: missing service id");
      return null;
    }
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
    log.atFine()
        .with(LogKey.SERVICE_NAME, serviceLock.getServiceName())
        .log("Attempting to relock service");
    try {
      serviceLock.setExpiry(ZonedDateTime.now().plus(lockDuration));
      serviceLockRepository.save(serviceLock);
      log.atFine().with(LogKey.SERVICE_NAME, serviceLock.getServiceName()).log("Relocked service");
      return true;
    } catch (Exception ex) {
      log.atSevere()
          .with(LogKey.SERVICE_NAME, serviceLock.getServiceName())
          .withCause(ex)
          .log("Failed to relock service");
      return false;
    }
  }

  /**
   * Unlocks the service.
   *
   * @param serviceLock the service lock
   */
  public void unlockService(ServiceLockEntity serviceLock) {
    log.atFine()
        .with(LogKey.SERVICE_NAME, serviceLock.getServiceName())
        .log("Attempting to unlock service");
    try {
      serviceLockRepository.delete(serviceLock);
      log.atFine().with(LogKey.SERVICE_NAME, serviceLock.getServiceName()).log("Unlocked service");
    } catch (Exception ex) {
      log.atSevere()
          .with(LogKey.SERVICE_NAME, serviceLock.getServiceName())
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
    // Catch DataIntegrityViolationException that my be thrown during commit.
    try {
      return lockService.lockProcessThrowsDataIntegrityViolationException(
          serviceLock, pipelineName, processId);
    } catch (DataIntegrityViolationException ex) {
      // Locking a process may throw DataIntegrityViolationException during commit.
      log.atSevere()
          .with(LogKey.SERVICE_NAME, serviceLock.getServiceName())
          .with(LogKey.PIPELINE_NAME, pipelineName)
          .with(LogKey.PROCESS_ID, processId)
          // .withCause(ex)
          .log("Failed to lock process");
      return false;
    }
  }

  public boolean lockProcessThrowsDataIntegrityViolationException(
      ServiceLockEntity serviceLock, String pipelineName, String processId) {
    log.atFine()
        .with(LogKey.SERVICE_NAME, serviceLock.getServiceName())
        .with(LogKey.PIPELINE_NAME, pipelineName)
        .with(LogKey.PROCESS_ID, processId)
        .log("Attempting to lock process:");
    removeExpiredProcessLock(pipelineName, processId);

    if (isProcessLocked(pipelineName, processId)) {
      return false;
    }

    ProcessLockEntity processLock = new ProcessLockEntity();
    processLock.setServiceId(serviceLock.getServiceId());
    processLock.setPipelineName(pipelineName);
    processLock.setProcessId(processId);
    processLockRepository.save(processLock);
    log.atFine()
        .with(LogKey.SERVICE_NAME, serviceLock.getServiceName())
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
    Optional<ServiceLockEntity> serviceLock =
        serviceLockRepository.findById(processLock.get().getServiceId());
    if (!serviceLock.isPresent()) {
      // Remove process lock as service lock does not exist.
      return processLockRepository.deleteByPipelineNameAndProcessId(pipelineName, processId) > 0;
    }
    if (!serviceLock.get().getExpiry().isAfter(ZonedDateTime.now())) {
      // Remove process lock as service lock has expired.
      return processLockRepository.deleteByPipelineNameAndProcessId(pipelineName, processId) > 0;
    }
    return false;
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
    log.atFine()
        .with(LogKey.SERVICE_NAME, serviceLock.getServiceName())
        .with(LogKey.PIPELINE_NAME, pipelineName)
        .with(LogKey.PROCESS_ID, processId)
        .log("Attempting to unlock process");
    try {
      ProcessLockEntity processLock = new ProcessLockEntity();
      processLock.setServiceId(serviceLock.getServiceId());
      processLock.setPipelineName(pipelineName);
      processLock.setProcessId(processId);
      processLockRepository.delete(processLock);
      log.atFine()
          .with(LogKey.SERVICE_NAME, serviceLock.getServiceName())
          .with(LogKey.PIPELINE_NAME, pipelineName)
          .with(LogKey.PROCESS_ID, processId)
          .log("Unlocked process");
      return true;
    } catch (Exception ex) {
      log.atSevere()
          .with(LogKey.SERVICE_NAME, serviceLock.getServiceName())
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
   * @param serviceLock the service lock
   */
  public void unlockProcesses(ServiceLockEntity serviceLock) {
    log.atFine()
        .with(LogKey.SERVICE_NAME, serviceLock.getServiceName())
        .log("Attempting to unlock processes");
    try {
      processLockRepository.deleteByServiceId(serviceLock.getServiceId());
      log.atFine().with(LogKey.SERVICE_NAME, serviceLock.getServiceName()).log("Unlock processes");
    } catch (Exception ex) {
      log.atSevere()
          .with(LogKey.SERVICE_NAME, serviceLock.getServiceName())
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
    Optional<ServiceLockEntity> serviceLock =
        serviceLockRepository.findById(processLock.get().getServiceId());
    if (!serviceLock.isPresent()) {
      return false;
    }
    return serviceLock.get().getExpiry().isAfter(ZonedDateTime.now());
  }

  /**
   * Returns the lock duration.
   *
   * @return the lock duration.
   */
  public Duration getLockDuration() {
    return lockDuration;
  }

  public List<ServiceLockEntity> getServiceLocksByServiceName(String serviceName) {
    Optional<ServiceLockEntity> lock = serviceLockRepository.findByServiceName(serviceName);
    if (lock.isPresent()) {
      return Arrays.asList(lock.get());
    }
    return Collections.emptyList();
  }
}
