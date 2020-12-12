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

import java.util.*;
import java.util.stream.Collectors;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import lombok.extern.flogger.Flogger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import pipelite.configuration.LauncherConfiguration;
import pipelite.configuration.StageConfiguration;
import pipelite.configuration.WebConfiguration;
import pipelite.launcher.*;
import pipelite.service.*;

@Configuration
@ComponentScan
@EnableAutoConfiguration
@Flogger
public class Application {

  @Autowired private WebConfiguration webConfiguration;
  @Autowired private LauncherConfiguration launcherConfiguration;
  @Autowired private StageConfiguration stageConfiguration;
  @Autowired private ProcessFactoryService processFactoryService;
  @Autowired private ProcessSourceService processSourceService;
  @Autowired private ScheduleService scheduleService;
  @Autowired private ProcessService processService;
  @Autowired private StageService stageService;
  @Autowired private LockService lockService;
  @Autowired private MailService mailService;
  @Autowired private ConfigurableApplicationContext applicationContext;

  private PipeliteServiceManager serverManager;
  private List<PipeliteLauncher> launchers;
  private PipeliteScheduler scheduler;
  private PipeliteUnlocker unlocker;
  private boolean shutdown;

  @PostConstruct
  private void init() {
    startUp();
    run();
  }

  /** Stops all services. */
  @PreDestroy
  public synchronized void stop() {
    log.atInfo().log("Stopping pipelite services");
    if (serverManager != null) {
      serverManager.shutdown();
    }
    launchers = null;
    scheduler = null;
    unlocker = null;
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
    serverManager = new PipeliteServiceManager();
    launchers = new ArrayList<>();
    try {
      log.atInfo().log("Starting pipelite services");
      // Check pipeline names.
      if (launcherConfiguration.getPipelineName() != null) {
        List<String> pipelineNames =
            Arrays.stream(launcherConfiguration.getPipelineName().split(","))
                .map(s -> s.trim())
                .collect(Collectors.toList());
        for (String pipelineName : pipelineNames) {
          processFactoryService.create(pipelineName);
        }
        // Create services.
        for (String pipelineName : pipelineNames) {
          PipeliteLauncher launcher = createLauncher(pipelineName);
          launchers.add(launcher);
          serverManager.addService(launcher);
        }
      }
      if (launcherConfiguration.getSchedulerName() != null) {
        scheduler = createScheduler();
        serverManager.addService(scheduler);
      }
      if (launcherConfiguration.getUnlockerName() != null) {
        unlocker = createUnlocker();
        serverManager.addService(unlocker);
      }
    } catch (Exception ex) {
      log.atSevere().withCause(ex).log("Unexpected exception when starting pipelite services");
      throw new RuntimeException(ex);
    }
    if (serverManager.getServiceCount() == 0) {
      log.atSevere().log("No pipelite services configured");
      shutDown();
    }
  }

  private PipeliteScheduler createScheduler() {
    return DefaultPipeliteScheduler.create(
        launcherConfiguration,
        stageConfiguration,
        lockService,
        processFactoryService,
        processService,
        scheduleService,
        stageService,
        mailService);
  }

  private PipeliteLauncher createLauncher(String pipelineName) {
    return DefaultPipeliteLauncher.create(
        webConfiguration,
        launcherConfiguration,
        stageConfiguration,
        lockService,
        processFactoryService,
        processSourceService,
        processService,
        stageService,
        mailService,
        pipelineName);
  }

  private PipeliteUnlocker createUnlocker() {
    return new PipeliteUnlocker(launcherConfiguration, lockService);
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
    if (scheduler == null || !scheduler.isRunning()) {
      return Collections.emptyList();
    }
    return Arrays.asList(scheduler);
  }

  public Collection<PipeliteService> getRunningServices() {
    if (serverManager == null) {
      return Collections.emptyList();
    }
    return serverManager.getServices();
  }
}
