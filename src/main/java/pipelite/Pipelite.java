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
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import lombok.extern.flogger.Flogger;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ConfigurableApplicationContext;
import picocli.CommandLine;
import pipelite.configuration.LauncherConfiguration;
import pipelite.launcher.PipeliteLauncher;
import pipelite.launcher.PipeliteScheduler;
import pipelite.launcher.ServerManager;
import pipelite.service.ProcessFactoryService;

@Flogger
public class Pipelite {

  private final PipeliteOptions options;
  private final List<PipeliteLauncher> launchers = new ArrayList<>();
  private final PipeliteScheduler scheduler;

  public Pipelite(PipeliteOptions options) {
    this.options = options;
    ConfigurableApplicationContext context =
        SpringApplication.run(Application.class, new String[] {});

    LauncherConfiguration launcherConfiguration = context.getBean(LauncherConfiguration.class);
    if (launcherConfiguration.getMode() == PipeliteMode.LAUNCHER) {
      if (launcherConfiguration.getPipelineName() == null) {
        throw new IllegalArgumentException("Missing pipeline name");
      }

      List<String> pipelineNames =
          Arrays.stream(launcherConfiguration.getPipelineName().split(","))
              .map(s -> s.trim())
              .collect(Collectors.toList());
      if (pipelineNames.isEmpty()) {
        throw new IllegalArgumentException("Missing pipeline name");
      }
      ProcessFactoryService processFactoryService = context.getBean(ProcessFactoryService.class);
      for (String pipelineName : pipelineNames) {
        // Check that the pipeline name is valid.
        processFactoryService.create(pipelineName);
      }
      for (String pipelineName : pipelineNames) {
        PipeliteLauncher launcher = context.getBean(PipeliteLauncher.class);
        launcher.setPipelineName(pipelineName);
        launchers.add(launcher);
      }
      scheduler = null;
    } else if (launcherConfiguration.getMode() == PipeliteMode.SHCEDULER) {
      scheduler = context.getBean(PipeliteScheduler.class);
    } else {
      scheduler = null;
    }
  }

  /**
   * Runs pipelite with the given command line arguments.
   *
   * @param args the command line arguments
   * @return the exit code
   */
  public static int run(String[] args) {
    PipeliteOptions options = parse(args);
    if (options == null) {
      return 1;
    }
    return new Pipelite(options).run();
  }

  /**
   * Creates the pipeline options from command line arguments.
   *
   * @param args the command line arguments
   * @return the pipelite options or null in case of invalid command line arguments.
   */
  public static PipeliteOptions parse(String[] args) {
    PipeliteOptions options = new PipeliteOptions();

    CommandLine commandLine;
    CommandLine.ParseResult parseResult;

    try {
      commandLine = new CommandLine(options);
    } catch (Exception ex) {
      System.out.println(ex.getMessage());
      return null;
    }

    try {
      parseResult = commandLine.parseArgs(args);
    } catch (Exception ex) {
      System.out.println(ex.getMessage());
      commandLine.usage(System.out);
      return null;
    }

    if (parseResult.isUsageHelpRequested()) {
      commandLine.usage(System.out);
      return null;
    }

    return options;
  }

  /**
   * Creates the pipelite object from the pipelite options.
   *
   * @param options the pipeline options
   * @return the pipelite object
   */
  public static Pipelite create(PipeliteOptions options) {
    return new Pipelite(options);
  }

  /**
   * Runs pipelite.
   *
   * @return the exit code
   */
  public int run() {
    try {

      if (!launchers.isEmpty()) {
        List<Callable<Object>> callables = new ArrayList<>();
        for (PipeliteLauncher launcher : launchers) {
          callables.add(
              () -> {
                if (options.removeLocks) {
                  launcher.removeLocks();
                }
                ServerManager.run(launcher, launcher.serviceName());
                return null;
              });
        }
        ExecutorService executorService = Executors.newCachedThreadPool();
        executorService.invokeAll(callables);
      }
      if (scheduler != null) {
        if (options.removeLocks) {
          scheduler.removeLocks();
        }
        ServerManager.run(scheduler, scheduler.serviceName());
      }
      return 0;
    } catch (Exception ex) {
      log.atSevere().withCause(ex).log("Unexpected exception when running pipelite");
      return 1;
    }
  }

  public PipeliteOptions getOptions() {
    return options;
  }

  public List<PipeliteLauncher> getLaunchers() {
    return launchers;
  }

  public PipeliteScheduler getScheduler() {
    return scheduler;
  }
}
