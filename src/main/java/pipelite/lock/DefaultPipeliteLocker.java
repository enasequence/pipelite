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
import org.springframework.util.Assert;
import pipelite.configuration.AdvancedConfiguration;
import pipelite.entity.ServiceLockEntity;
import pipelite.service.LockService;
import pipelite.time.Time;

import java.time.Duration;

@Flogger
public class DefaultPipeliteLocker implements PipeliteLocker {

  private final LockService lockService;
  private final String serviceName;
  private final ServiceLockEntity lock;
  private final Thread renewLockThread;

  public DefaultPipeliteLocker(
      AdvancedConfiguration advancedConfiguration, LockService lockService, String serviceName) {
    Assert.notNull(advancedConfiguration, "Missing advanced configuration");
    Assert.notNull(lockService, "Missing lock service");
    Assert.notNull(serviceName, "Missing service name");
    this.lockService = lockService;
    this.serviceName = serviceName;
    this.lock = lockService();
    Duration lockFrequency = getLockFrequency(advancedConfiguration);
    this.renewLockThread =
        new Thread(
            () -> {
              while (true) {
                Time.wait(lockFrequency);
                this.renewLock();
              }
            });
    this.renewLockThread.start();
  }

  @Override
  public void close() {
    this.renewLockThread.stop();
    if (lock == null) {
      return;
    }
    unlockService();
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

  private ServiceLockEntity lockService() {
    Assert.notNull(serviceName, "Missing service name");
    log.atFine().log("Locking service: " + serviceName);
    ServiceLockEntity lock = LockService.lockService(lockService, serviceName);
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
      throw new RuntimeException("Could not re-lock service: " + serviceName);
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
