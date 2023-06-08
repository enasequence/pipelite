/*
 * Copyright 2020-2022 EMBL - European Bioinformatics Institute
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package pipelite;

import javax.annotation.PostConstruct;
import lombok.extern.flogger.Flogger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.retry.annotation.EnableRetry;
import org.springframework.scheduling.annotation.EnableScheduling;
import pipelite.manager.ProcessRunnerPoolManager;

@Configuration
@ComponentScan
@EnableAutoConfiguration
@EnableRetry
@Flogger
@EnableScheduling
public class PipeliteApplication {

  @Autowired ProcessRunnerPoolManager processRunnerPoolManager;

  @PostConstruct
  private void run() {
    processRunnerPoolManager.createPools();
    processRunnerPoolManager.startPools();
  }
}
