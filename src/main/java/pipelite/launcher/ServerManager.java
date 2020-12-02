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
package pipelite.launcher;

import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.Service;
import com.google.common.util.concurrent.ServiceManager;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import lombok.extern.flogger.Flogger;
import org.springframework.util.Assert;
import pipelite.launcher.lock.PipeliteUnlocker;

@Flogger
public class ServerManager {

  public static final int STOP_WAIT_MAX_SECONDS = 30;
  public static final int STOP_WAIT_MIN_SECONDS = 25;

  private final Set<Service> services = new HashSet<>();
  private ServiceManager manager;

  public ServerManager add(PipeliteUnlocker service) {
    Assert.isNull(manager, "Unable to add new pipelite services");
    Assert.notNull(service, "Missing pipelite service");
    log.atInfo().log("Adding new pipelite service: " + service.serviceName());
    if (!services.add(service)) {
      throw new RuntimeException("Non unique pipelite service name: " + service.serviceName());
    }
    return this;
  }

  public ServerManager add(ProcessLauncherService service) {
    Assert.isNull(manager, "Unable to add new pipelite services");
    Assert.notNull(service, "Missing pipelite service");
    log.atInfo().log("Adding new pipelite service: " + service.serviceName());
    if (!services.add(service)) {
      throw new RuntimeException("Non unique pipelite service name: " + service.serviceName());
    }
    return this;
  }

  public void run() {
    Assert.isNull(manager, "Unable to run new pipelite services");
    manager = new ServiceManager(services);
    manager.addListener(
        new ServiceManager.Listener() {
          public void stopped() {}

          public void healthy() {}

          public void failure(Service service) {
            log.atSevere().withCause(service.failureCause()).log("Pipelite service has failed");
            stop();
          }
        },
        MoreExecutors.directExecutor());
    Runtime.getRuntime().addShutdownHook(new Thread(() -> stop()));
    log.atInfo().log("Starting pipelite services");
    manager.startAsync().awaitStopped();
    log.atInfo().log("Pipelite services have stopped");
  }

  public void stop() {
    log.atInfo().log("Stopping all pipelite services");
    try {
      manager.stopAsync().awaitStopped(STOP_WAIT_MAX_SECONDS, TimeUnit.SECONDS);
    } catch (TimeoutException timeout) {
    }
  }
}
