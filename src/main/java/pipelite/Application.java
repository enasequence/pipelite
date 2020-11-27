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

import com.google.common.util.concurrent.Service;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import lombok.extern.flogger.Flogger;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import pipelite.configuration.LauncherConfiguration;
import pipelite.launcher.PipeliteLauncher;
import pipelite.launcher.PipeliteScheduler;
import pipelite.launcher.PipeliteUnlocker;
import pipelite.launcher.ServerManager;
import pipelite.service.*;

@Configuration
@ComponentScan
@EnableAutoConfiguration
@Flogger
public class Application {

  @Autowired private LauncherConfiguration launcherConfiguration;
  @Autowired private ProcessFactoryService processFactoryService;
  @Autowired private PipeliteUnlocker pipeliteUnlocker;
  @Autowired private ObjectProvider<PipeliteLauncher> pipeliteLauncherObjectProvider;
  @Autowired private ObjectProvider<PipeliteScheduler> pipeliteSchedulerObjectProvider;
  private List<PipeliteLauncher> launchers = new ArrayList<>();
  private PipeliteScheduler scheduler;

  List<Service> services = new ArrayList<>();
  ExecutorService executorService = Executors.newCachedThreadPool();

  @PostConstruct
  private void construct() {
    init();
    run();
  }

  @PreDestroy
  private void destroy() {
    services.stream().forEach(service -> service.stopAsync());
    executorService.shutdown();
    try {
      executorService.awaitTermination(ServerManager.FORCE_STOP_WAIT_SECONDS - 1, TimeUnit.SECONDS);
    } catch (InterruptedException ex) {
      executorService.shutdownNow();
    }
  }

  private void init() {
    try {
      log.atInfo().log("Initialising pipelite services");
      if (launcherConfiguration.getPipelineName() != null) {
        List<String> pipelineNames =
            Arrays.stream(launcherConfiguration.getPipelineName().split(","))
                .map(s -> s.trim())
                .collect(Collectors.toList());
        for (String pipelineName : pipelineNames) {
          // Check pipeline name.
          processFactoryService.create(pipelineName);
        }
        for (String pipelineName : pipelineNames) {
          PipeliteLauncher launcher = pipeliteLauncherObjectProvider.getObject();
          launcher.startUp(pipelineName);
          launchers.add(launcher);
        }
      }
      if (launcherConfiguration.getSchedulerName() != null) {
        scheduler = pipeliteSchedulerObjectProvider.getObject();
      }
    } catch (Exception ex) {
      log.atSevere().withCause(ex).log("Unexpected exception when initialising pipelite services");
    }
  }

  private void run() {
    try {
      log.atInfo().log("Starting pipelite services");
      services.add(pipeliteUnlocker);
      executorService.submit(
          () -> ServerManager.run(pipeliteUnlocker, pipeliteUnlocker.serviceName()));

      for (PipeliteLauncher launcher : launchers) {
        services.add(launcher);
        executorService.submit(() -> ServerManager.run(launcher, launcher.serviceName()));
      }

      if (scheduler != null) {
        services.add(scheduler);
        executorService.submit(() -> ServerManager.run(scheduler, scheduler.serviceName()));
      }
    } catch (Exception ex) {
      log.atSevere().withCause(ex).log("Unexpected exception when starting pipelite services");
    }
  }

  public List<PipeliteLauncher> getLaunchers() {
    return launchers;
  }

  public PipeliteScheduler getScheduler() {
    return scheduler;
  }
}
