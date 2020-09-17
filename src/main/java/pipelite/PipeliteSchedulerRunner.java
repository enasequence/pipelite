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

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import pipelite.launcher.PipeliteScheduler;
import pipelite.launcher.ServerManager;

@SpringBootApplication
public class PipeliteSchedulerRunner {

  public static void run() {
    ConfigurableApplicationContext context =
        SpringApplication.run(PipeliteSchedulerRunner.class, new String[] {});
    PipeliteScheduler pipeliteScheduler = context.getBean(PipeliteScheduler.class);
    ServerManager.run(pipeliteScheduler, pipeliteScheduler.serviceName());
  }
}
