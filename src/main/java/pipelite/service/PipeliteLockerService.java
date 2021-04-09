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
import javax.annotation.PreDestroy;
import lombok.extern.flogger.Flogger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.Assert;
import pipelite.configuration.AdvancedConfiguration;
import pipelite.configuration.ServiceConfiguration;
import pipelite.entity.ServiceLockEntity;
import pipelite.exception.PipeliteException;
import pipelite.time.Time;

@Service
@Transactional(propagation = Propagation.REQUIRES_NEW)
@Flogger
public class PipeliteLockerService {

  private final LockService lockService;
  private final String serviceName;
  private final ServiceLockEntity serviceLock;
  private final Thread relockServiceThread;

  public PipeliteLockerService(
      @Autowired ServiceConfiguration serviceConfiguration,
      @Autowired AdvancedConfiguration advancedConfiguration,
      @Autowired LockService lockService) {
    this.lockService = lockService;
    this.serviceName = serviceConfiguration.getName();
    this.serviceLock = lockService(serviceConfiguration.isForce());
    Duration lockFrequency = getLockFrequency(advancedConfiguration);
    this.relockServiceThread =
        new Thread(
            () -> {
              while (!Thread.interrupted()) {
                try {
                  Time.wait(lockFrequency);
                } catch (Exception ex) {
                  log.atInfo().log("Service lock renewal interrupted");
                  break;
                }
                try {
                  this.relockService();
                } catch (Exception ex) {
                  log.atSevere().log("Unexpected exception when renewing service lock");
                }
              }
            });
    this.relockServiceThread.start();
  }

  @PreDestroy
  private void close() {
    try {
      if (this.relockServiceThread != null) {
        this.relockServiceThread.interrupt();
      }
      unlockService();
    } catch (Exception ex) {
      log.atSevere().withCause(ex).log("Unexpected exception when closing pipelite locker");
    }
  }

  private Duration getLockFrequency(AdvancedConfiguration advancedConfiguration) {
    return advancedConfiguration.getLockFrequency() != null
        ? advancedConfiguration.getLockFrequency()
        : AdvancedConfiguration.DEFAULT_LOCK_FREQUENCY;
  }

  private ServiceLockEntity lockService(boolean force) {
    log.atFine().log("Locking service: " + serviceName);
    ServiceLockEntity lock = LockService.lockService(lockService, serviceName);
    if (lock == null && force) {
      log.atWarning().log(
          "Forceful startup requested. Removing existing locks for service: " + serviceName);
      unlockService();
      lock = LockService.lockService(lockService, serviceName);
    }
    if (lock == null) {
      throw new RuntimeException("Could not lock service " + serviceName);
    }
    return lock;
  }

  private void unlockService() {
    try {
      log.atFine().log("Unlocking service: " + serviceName);
      lockService.unlockProcesses(serviceName);
      lockService.unlockService(serviceName);
    } catch (Exception ex) {
      log.atSevere().log("Failed to unlock service: " + serviceName);
    }
  }

  private void relockService() {
    log.atFine().log("Relocking service: " + serviceName);
    if (!lockService.relockService(serviceLock)) {
      throw new PipeliteException("Could not relock service: " + serviceName);
    }
  }

  public boolean lockProcess(String pipelineName, String processId) {
    Assert.notNull(serviceLock, "Missing lock");
    Assert.notNull(pipelineName, "Missing pipeline name");
    Assert.notNull(processId, "Missing process id");
    return LockService.lockProcess(lockService, serviceLock, pipelineName, processId);
  }

  public void unlockProcess(String pipelineName, String processId) {
    Assert.notNull(serviceLock, "Missing lock");
    Assert.notNull(pipelineName, "Missing pipeline name");
    Assert.notNull(processId, "Missing process id");
    lockService.unlockProcess(serviceLock, pipelineName, processId);
  }
}
