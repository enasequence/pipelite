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
package pipelite.manager;

import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.Service;
import com.google.common.util.concurrent.ServiceManager;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import javax.annotation.PreDestroy;
import lombok.extern.flogger.Flogger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import pipelite.Pipeline;
import pipelite.Schedule;
import pipelite.configuration.PipeliteConfiguration;
import pipelite.exception.PipeliteException;
import pipelite.launcher.DefaultPipeliteLauncher;
import pipelite.launcher.DefaultPipeliteScheduler;
import pipelite.launcher.PipeliteLauncher;
import pipelite.launcher.PipeliteScheduler;
import pipelite.metrics.PipeliteMetrics;
import pipelite.service.PipeliteServices;

@Flogger
@Component
public class RegisteredServiceManager {

  private final PipeliteConfiguration pipeliteConfiguration;
  private final PipeliteServices pipeliteServices;
  private final PipeliteMetrics pipeliteMetrics;

  private final Set<RegisteredService> services = new HashSet<>();
  private ServiceManager serviceManager;
  private State state;

  private enum State {
    STOPPED,
    INITIALISED,
    RUNNING
  }

  public RegisteredServiceManager(
      @Autowired PipeliteConfiguration pipeliteConfiguration,
      @Autowired PipeliteServices pipeliteServices,
      @Autowired PipeliteMetrics pipeliteMetrics) {
    this.pipeliteConfiguration = pipeliteConfiguration;
    this.pipeliteServices = pipeliteServices;
    this.pipeliteMetrics = pipeliteMetrics;
    this.state = State.STOPPED;
  }

  public Collection<RegisteredService> getServices() {
    return services;
  }

  protected synchronized RegisteredServiceManager addService(RegisteredService service) {
    if (services.contains(service)) {
      throw new RuntimeException("Pipelite service name is not unique: " + service.serviceName());
    }
    services.add(service);
    return this;
  }

  private void clear() {
    pipeliteServices.launcher().clearPipeliteScheduler();
    pipeliteServices.launcher().clearPipeliteLaunchers();
    services.clear();
    serviceManager = null;
  }

  /** Initialises pipelite services. */
  public synchronized void init() {
    log.atInfo().log("Initialising pipelite services");

    if (state != State.STOPPED) {
      log.atWarning().log(
          "Failed to initialise pipelite services because services are not stopped");
      return;
    }

    try {
      // Create launchers for unscheduled pipelines.
      for (Pipeline pipeline :
          pipeliteServices.registeredPipeline().getRegisteredPipelines(Pipeline.class)) {
        PipeliteLauncher launcher = createLauncher(pipeline.pipelineName());
        log.atInfo().log("Adding pipelite launcher: " + launcher.serviceName());
        pipeliteServices.launcher().addPipeliteLauncher(launcher);
        addService(launcher);
      }

      // Create scheduler for scheduled pipelines.
      if (pipeliteServices.registeredPipeline().isScheduler()) {
        PipeliteScheduler pipeliteScheduler =
            createScheduler(
                pipeliteServices.registeredPipeline().getRegisteredPipelines(Schedule.class));
        log.atInfo().log("Adding pipelite scheduler: " + pipeliteScheduler.serviceName());
        pipeliteServices.launcher().setPipeliteScheduler(pipeliteScheduler);
        addService(pipeliteScheduler);
      }

      initServices();
    } catch (Exception ex) {
      log.atSevere().withCause(ex).log("Unexpected exception when initialising pipelite services");
      clear();
      throw new PipeliteException(ex);
    }
  }

  protected synchronized void initServices() {
    if (services.isEmpty()) {
      log.atSevere().log("No pipelite services configured");
      return;
    }

    try {
      serviceManager = new ServiceManager(services);
      serviceManager.addListener(
          new ServiceManager.Listener() {
            public void stopped() {}

            public void healthy() {}

            public void failure(Service service) {
              String serviceName = ((RegisteredService) service).serviceName();
              log.atSevere().withCause(service.failureCause()).log(
                  "Pipelite service has failed: " + serviceName);
              pipeliteServices
                  .internalError()
                  .saveInternalError(serviceName, this.getClass(), service.failureCause());
              stop();
            }
          },
          MoreExecutors.directExecutor());

    } catch (Exception ex) {
      log.atSevere().withCause(ex).log("Unexpected exception when initialising pipelite services");
      clear();
      throw new PipeliteException(ex);
    }

    state = State.INITIALISED;
    log.atInfo().log("Initialised pipelite services");
  }

  /** Starts pipelite services. */
  public synchronized void start() {
    log.atInfo().log("Starting pipelite services");

    if (state != State.INITIALISED) {
      log.atWarning().log("Failed to start pipelite services because services are not initialised");
      return;
    }

    serviceManager.startAsync();

    state = State.RUNNING;
    log.atInfo().log("Started pipelite services");
  }

  public synchronized void awaitStopped() {
    if (state != State.RUNNING) {
      log.atWarning().log(
          "Failed to wait for pipelite services to stop because services are not running");
      return;
    }
    serviceManager.awaitStopped();
    state = State.STOPPED;
  }

  @PreDestroy
  /** Stops pipelite services. */
  public synchronized void stop() {
    if (state != State.RUNNING) {
      return;
    }

    log.atInfo().log(
        "Stopping pipelite services. Shutdown period is + "
            + pipeliteConfiguration.service().getShutdownPeriod().getSeconds()
            + " seconds.");

    try {
      serviceManager
          .stopAsync()
          .awaitStopped(
              pipeliteConfiguration.service().getShutdownPeriod().getSeconds(), TimeUnit.SECONDS);
    } catch (TimeoutException ignored) {
    }
    clear();
    log.atInfo().log("Stopped pipelite services");
    state = State.STOPPED;
  }

  /** Terminates all running processes. */
  public synchronized void terminateProcesses() {
    log.atInfo().log("Terminating all running processes");
    services.forEach(service -> service.terminateProcesses());
  }

  private PipeliteScheduler createScheduler(List<Schedule> schedules) {
    return DefaultPipeliteScheduler.create(
        pipeliteConfiguration, pipeliteServices, pipeliteMetrics, schedules);
  }

  private PipeliteLauncher createLauncher(String pipelineName) {
    return DefaultPipeliteLauncher.create(
        pipeliteConfiguration, pipeliteServices, pipeliteMetrics, pipelineName);
  }
}
