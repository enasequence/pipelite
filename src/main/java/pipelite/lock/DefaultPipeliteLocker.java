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

import java.time.Duration;
import lombok.extern.flogger.Flogger;
import org.springframework.util.Assert;
import pipelite.configuration.AdvancedConfiguration;
import pipelite.configuration.ServiceConfiguration;
import pipelite.entity.ServiceLockEntity;
import pipelite.exception.PipeliteException;
import pipelite.service.LockService;
import pipelite.time.Time;

@Flogger
public class DefaultPipeliteLocker implements PipeliteLocker {

  private final LockService lockService;
  private final String serviceName;
  private final ServiceLockEntity lock;
  private final Thread renewLockThread;

  public DefaultPipeliteLocker(
      ServiceConfiguration serviceConfiguration,
      AdvancedConfiguration advancedConfiguration,
      LockService lockService) {
    Assert.notNull(serviceConfiguration, "Missing service configuration");
    Assert.notNull(advancedConfiguration, "Missing advanced configuration");
    Assert.notNull(lockService, "Missing lock service");
    this.lockService = lockService;
    this.serviceName = serviceConfiguration.getName();
    Assert.notNull(serviceName, "Missing service name");
    this.lock = lockService(serviceConfiguration.isForce());
    Duration lockFrequency = getLockFrequency(advancedConfiguration);
    this.renewLockThread =
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
                  this.renewLock();
                } catch (Exception ex) {
                  log.atInfo().log("Unexpected exception when renewing service lock");
                }
              }
            });
    this.renewLockThread.start();
  }

  @Override
  public void close() {
    if (this.renewLockThread != null) {
      this.renewLockThread.interrupt();
    }
    if (lock != null) {
      unlockService();
    }
  }

  @Override
  public String getServiceName() {
    return serviceName;
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
      log.atWarning().log("Forcing locking service: " + serviceName);
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
      lockService.unlockProcesses(lock);
      lockService.unlockService(lock);
    } catch (Exception ex) {
      log.atSevere().log("Failed to unlock service: " + serviceName);
    }
  }

  private void renewLock() {
    log.atFine().log("Re-locking service: " + serviceName);
    if (!lockService.relockService(lock)) {
      throw new PipeliteException("Could not re-lock service: " + serviceName);
    }
  }

  @Override
  public boolean lockProcess(String pipelineName, String processId) {
    Assert.notNull(lock, "Missing lock");
    Assert.notNull(pipelineName, "Missing pipeline name");
    Assert.notNull(processId, "Missing process id");
    return LockService.lockProcess(lockService, lock, pipelineName, processId);
  }

  @Override
  public void unlockProcess(String pipelineName, String processId) {
    Assert.notNull(lock, "Missing lock");
    Assert.notNull(pipelineName, "Missing pipeline name");
    Assert.notNull(processId, "Missing process id");
    lockService.unlockProcess(lock, pipelineName, processId);
  }

  @Override
  public ServiceLockEntity getLock() {
    return lock;
  }
}
