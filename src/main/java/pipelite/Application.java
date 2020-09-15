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
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import pipelite.launcher.PipeliteLauncher;
import pipelite.launcher.PipeliteScheduler;
import pipelite.launcher.ServerManager;

@Flogger
@SpringBootApplication
public class Application implements CommandLineRunner {

  @Autowired PipeliteLauncher pipeliteLauncher;
  @Autowired PipeliteScheduler pipeliteScheduler;

  private static final String LAUNCHER = "LAUNCHER";
  private static final String SCHEDULER = "SCHEDULER";

  public static void main(String[] args) {
    SpringApplication.run(Application.class, args);
  }

  @Override
  public void run(String... args) {
    if (args.length == 0
        || !(args[0].equalsIgnoreCase(LAUNCHER) && args[0].equalsIgnoreCase(SCHEDULER))) {
      throw new RuntimeException(
          "Please provide either 'LAUNCHER' or 'SCHEDULER' as the first command line argument");
    }

    try {
      switch (args[0]) {
        case LAUNCHER:
          launcher();
          break;
        case SCHEDULER:
          scheduler();
          break;
      }
    } catch (Exception ex) {
      log.atSevere().withCause(ex).log("Uncaught exception");
      throw ex;
    }
  }

  private void launcher() {
    ServerManager.run(pipeliteLauncher, pipeliteLauncher.serviceName());
  }

  private void scheduler() {
    ServerManager.run(pipeliteScheduler, pipeliteScheduler.serviceName());
  }
}
