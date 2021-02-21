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
package pipelite;

import lombok.extern.flogger.Flogger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.retry.annotation.EnableRetry;
import pipelite.configuration.AdvancedConfiguration;
import pipelite.configuration.ExecutorConfiguration;
import pipelite.configuration.ServiceConfiguration;
import pipelite.launcher.*;
import pipelite.metrics.PipeliteMetrics;
import pipelite.service.*;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

@Configuration
@ComponentScan
@EnableAutoConfiguration
@EnableRetry
@Flogger
public class Application {

  @Autowired private ServiceConfiguration serviceConfiguration;
  @Autowired private AdvancedConfiguration advancedConfiguration;
  @Autowired private ExecutorConfiguration executorConfiguration;
  @Autowired private InternalErrorService internalErrorService;
  @Autowired private HealthCheckService healthCheckService;
  @Autowired private RegisteredPipelineService registeredPipelineService;
  @Autowired private PipeliteLockerService pipeliteLockerService;
  @Autowired private ScheduleService scheduleService;
  @Autowired private ProcessService processService;
  @Autowired private StageService stageService;
  @Autowired private MailService mailService;
  @Autowired private ConfigurableApplicationContext applicationContext;
  @Autowired private PipeliteMetrics metrics;

  private PipeliteServiceManager serverManager;
  private final List<PipeliteLauncher> launchers = new ArrayList<>();
  private final List<PipeliteScheduler> schedulers = new ArrayList<>();
  private boolean shutdown;

  @PostConstruct
  private void init() {
    startUp();
    run();
  }

  /** Stops all services. Called when the spring application is shutdown. */
  @PreDestroy
  public synchronized void stop() {
    log.atInfo().log("Stopping pipelite services");
    if (serverManager != null) {
      serverManager.shutdown();
    }
    launchers.clear();
    schedulers.clear();
  }

  /** Stops all services and terminates all running processes. */
  public synchronized void kill() {
    log.atInfo().log("Terminating all running processes");
    if (serverManager != null) {
      for (PipeliteService service : serverManager.getServices()) {
        service.terminate();
      }
    }
    stop();
  }

  /** Restarts all services. */
  public synchronized void restart() {
    log.atInfo().log("Restarting pipelite services");
    stop();
    startUp();
  }

  /** Shuts down the application. */
  public synchronized void shutDown() {
    shutdown = true;
    log.atInfo().log("Shutting down pipelite services");
    stop();
    applicationContext.close();
  }

  private synchronized void startUp() {
    if (shutdown) {
      return;
    }
    serverManager = new PipeliteServiceManager(serviceConfiguration, internalErrorService);
    launchers.clear();
    schedulers.clear();
    try {
      log.atInfo().log("Starting pipelite services");
      // Create launchers for unscheduled pipelines.

      PipeliteConfiguration pipeliteConfiguration =
          new PipeliteConfiguration(
              serviceConfiguration, advancedConfiguration, executorConfiguration, metrics);

      PipeliteServices pipeliteServices =
          new PipeliteServices(
              scheduleService,
              processService,
              stageService,
              mailService,
              pipeliteLockerService,
              registeredPipelineService,
              internalErrorService,
              healthCheckService);

      for (Pipeline pipeline : registeredPipelineService.getRegisteredPipelines(Pipeline.class)) {
        PipeliteLauncher launcher =
            createLauncher(pipeliteConfiguration, pipeliteServices, pipeline.pipelineName());
        launchers.add(launcher);
        serverManager.addService(launcher);
      }
      // Create scheduler for scheduled pipelines.
      if (registeredPipelineService.isScheduler()) {
        PipeliteScheduler scheduler =
            createScheduler(
                pipeliteConfiguration,
                pipeliteServices,
                registeredPipelineService.getRegisteredPipelines(Schedule.class));
        schedulers.add(scheduler);
        serverManager.addService(scheduler);
      }
    } catch (Exception ex) {
      log.atSevere().withCause(ex).log("Unexpected exception when starting pipelite services");
      throw new RuntimeException(ex);
    }
    if (serverManager.getServiceCount() == 0) {
      log.atSevere().log("No pipelite services configured");
      // shutDown();
    }
  }

  private PipeliteScheduler createScheduler(
      PipeliteConfiguration pipeliteConfiguration,
      PipeliteServices pipeliteServices,
      List<Schedule> schedules) {
    return DefaultPipeliteScheduler.create(pipeliteConfiguration, pipeliteServices, schedules);
  }

  private PipeliteLauncher createLauncher(
      PipeliteConfiguration pipeliteConfiguration,
      PipeliteServices pipeliteServices,
      String pipelineName) {
    return DefaultPipeliteLauncher.create(pipeliteConfiguration, pipeliteServices, pipelineName);
  }

  private void run() {
    if (shutdown) {
      return;
    }
    try {
      log.atInfo().log("Running pipelite services");

      serverManager.runAsync();

    } catch (Exception ex) {
      log.atSevere().withCause(ex).log("Unexpected exception when running pipelite services");
    }
  }

  public Collection<PipeliteLauncher> getRunningLaunchers() {
    return launchers.stream()
        .filter(pipeliteLauncher -> pipeliteLauncher.isRunning())
        .collect(Collectors.toList());
  }

  public Collection<PipeliteScheduler> getRunningSchedulers() {
    return schedulers.stream()
        .filter(pipeliteScheduler -> pipeliteScheduler.isRunning())
        .collect(Collectors.toList());
  }

  public boolean isRunningScheduler() {
    return !schedulers.isEmpty();
  }

  public boolean isShutdown() {
    return shutdown;
  }
}
