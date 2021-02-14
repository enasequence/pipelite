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
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import lombok.extern.flogger.Flogger;
import org.springframework.util.Assert;
import pipelite.configuration.ServiceConfiguration;
import pipelite.service.InternalErrorService;

@Flogger
public class PipeliteServiceManager {

  private final Set<PipeliteService> services = new HashSet<>();
  private final ServiceConfiguration serviceConfiguration;
  private final InternalErrorService internalErrorService;
  private ServiceManager manager;

  public PipeliteServiceManager(
      ServiceConfiguration serviceConfiguration, InternalErrorService internalErrorService) {
    this.serviceConfiguration = serviceConfiguration;
    this.internalErrorService = internalErrorService;
  }

  public PipeliteServiceManager addService(PipeliteService service) {
    Assert.isNull(manager, "Unable to add new pipelite services");
    Assert.notNull(service, "Missing pipelite service");
    log.atInfo().log("Adding new pipelite service: " + service.serviceName());
    if (!services.add(service)) {
      throw new RuntimeException("Non unique pipelite service name: " + service.serviceName());
    }
    return this;
  }

  public Collection<PipeliteService> getServices() {
    return services;
  }

  public int getServiceCount() {
    return services.size();
  }

  /** Runs pipelite service asynchronously. Returns after starting services. */
  public void runAsync() {
    run(false);
  }

  /** Runs pipelite service synchronously. Returns after services have stopped. */
  public void runSync() {
    run(true);
  }

  /**
   * Runs pipelite services.
   *
   * @param sync if false then returns after starting services. If true then returns after services
   *     have stopped.
   */
  private void run(boolean sync) {
    Assert.isNull(manager, "Unable to run new pipelite services");
    manager = new ServiceManager(services);
    manager.addListener(
        new ServiceManager.Listener() {
          public void stopped() {}

          public void healthy() {}

          public void failure(Service service) {
            String serviceName = ((PipeliteService) service).serviceName();
            log.atSevere().withCause(service.failureCause()).log(
                "Pipelite service has failed: " + serviceName);
            internalErrorService.saveInternalError(
                serviceName, null, this.getClass(), service.failureCause());
            shutdown();
          }
        },
        MoreExecutors.directExecutor());
    log.atInfo().log("Starting pipelite services");

    manager.startAsync();
    if (sync) {
      manager.awaitStopped();
    }
  }

  public void shutdown() {
    log.atInfo().log(
        "Stopping all pipelite services. Shutdown period is + "
            + serviceConfiguration.getShutdownPeriod().getSeconds()
            + " seconds.");
    if (manager == null) {
      return;
    }
    try {
      manager
          .stopAsync()
          .awaitStopped(serviceConfiguration.getShutdownPeriod().getSeconds(), TimeUnit.SECONDS);
    } catch (TimeoutException ignored) {
    }
    log.atInfo().log("Stopped pipelite services");
  }
}
