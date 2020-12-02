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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import lombok.extern.flogger.Flogger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import pipelite.configuration.LauncherConfiguration;
import pipelite.configuration.StageConfiguration;
import pipelite.launcher.PipeliteLauncher;
import pipelite.launcher.PipeliteScheduler;
import pipelite.launcher.ServerManager;
import pipelite.launcher.lock.PipeliteUnlocker;
import pipelite.service.*;

@Configuration
@ComponentScan
@EnableAutoConfiguration
@Flogger
public class Application {

  @Autowired private LauncherConfiguration launcherConfiguration;
  @Autowired private StageConfiguration stageConfiguration;
  @Autowired private ProcessFactoryService processFactoryService;
  @Autowired private ProcessSourceService processSourceService;
  @Autowired private ScheduleService scheduleService;
  @Autowired private ProcessService processService;
  @Autowired private StageService stageService;
  @Autowired private LockService lockService;
  @Autowired private MailService mailService;
  @Autowired private PipeliteUnlocker pipeliteUnlocker;

  private final ServerManager serverManager = new ServerManager();
  private final List<PipeliteLauncher> launchers = new ArrayList<>();
  private PipeliteScheduler scheduler;

  @PostConstruct
  private void construct() {
    init();
    run();
  }

  @PreDestroy
  private void destroy() {
    serverManager.stop();
  }

  private void init() {
    try {
      log.atInfo().log("Initialising pipelite services");
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
        serverManager.add(pipeliteUnlocker);
        for (String pipelineName : pipelineNames) {
          PipeliteLauncher launcher =
              new PipeliteLauncher(
                  launcherConfiguration,
                  stageConfiguration,
                  processFactoryService,
                  processSourceService,
                  processService,
                  stageService,
                  lockService,
                  mailService,
                  pipelineName);
          launchers.add(launcher);
          serverManager.add(launcher);
        }
      }
      if (launcherConfiguration.getSchedulerName() != null) {
        scheduler =
            new PipeliteScheduler(
                launcherConfiguration,
                stageConfiguration,
                processFactoryService,
                scheduleService,
                processService,
                stageService,
                lockService,
                mailService);
        serverManager.add(scheduler);
      }
    } catch (Exception ex) {
      log.atSevere().withCause(ex).log("Unexpected exception when initialising pipelite services");
      throw new RuntimeException(ex);
    }
  }

  private void run() {
    try {
      log.atInfo().log("Starting pipelite services");
      serverManager.run();
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
