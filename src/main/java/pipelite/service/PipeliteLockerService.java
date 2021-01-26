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

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import lombok.extern.flogger.Flogger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import pipelite.configuration.AdvancedConfiguration;
import pipelite.configuration.ServiceConfiguration;
import pipelite.lock.DefaultPipeliteLocker;
import pipelite.lock.PipeliteLocker;

@Service
@Transactional(propagation = Propagation.REQUIRES_NEW)
@Flogger
public class PipeliteLockerService {

  private final ServiceConfiguration serviceConfiguration;
  private final AdvancedConfiguration advancedConfiguration;
  private final LockService lockService;

  private PipeliteLocker pipeliteLocker;

  public PipeliteLockerService(
      @Autowired ServiceConfiguration serviceConfiguration,
      @Autowired AdvancedConfiguration advancedConfiguration,
      @Autowired LockService lockService) {
    this.serviceConfiguration = serviceConfiguration;
    this.advancedConfiguration = advancedConfiguration;
    this.lockService = lockService;
  }

  @PostConstruct
  private void init() {
    pipeliteLocker =
        new DefaultPipeliteLocker(serviceConfiguration, advancedConfiguration, lockService);
  }

  @PreDestroy
  public synchronized void close() {
    if (pipeliteLocker != null) {
      try {
        pipeliteLocker.close();
      } catch (Exception ex) {
        log.atSevere().withCause(ex).log("Unexpected exception when closing pipelite locker");
      }
    }
  }

  public PipeliteLocker getPipeliteLocker() {
    return pipeliteLocker;
  }
}
