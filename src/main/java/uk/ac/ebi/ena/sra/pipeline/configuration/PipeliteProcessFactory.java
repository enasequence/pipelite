/*
 * Copyright 2018-2019 EMBL - European Bioinformatics Institute
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package uk.ac.ebi.ena.sra.pipeline.configuration;

import lombok.Value;
import pipelite.ApplicationConfiguration;
import pipelite.configuration.TaskConfiguration;
import pipelite.entity.PipeliteProcess;
import pipelite.service.PipeliteProcessService;
import pipelite.service.PipeliteStageService;
import pipelite.resolver.ExceptionResolver;
import pipelite.service.PipeliteLockService;
import uk.ac.ebi.ena.sra.pipeline.launcher.PipeliteLauncher.ProcessLauncherInterface;
import uk.ac.ebi.ena.sra.pipeline.launcher.PipeliteLauncher.ProcessFactory;
import uk.ac.ebi.ena.sra.pipeline.launcher.ProcessLauncher;

@Value
public class PipeliteProcessFactory implements ProcessFactory {

  private final String launcherName;
  private final ApplicationConfiguration applicationConfiguration;
  private final PipeliteLockService locker;
  private final PipeliteProcessService pipeliteProcessService;
  private final PipeliteStageService pipeliteStageService;

  @Override
  public ProcessLauncherInterface create(PipeliteProcess pipeliteProcess) {
    ExceptionResolver resolver = applicationConfiguration.processConfiguration.createResolver();

    ProcessLauncher process =
        new ProcessLauncher(
            launcherName,
            pipeliteProcess,
            resolver,
            locker,
            pipeliteProcessService,
            pipeliteStageService,
            applicationConfiguration.taskConfiguration);
    // TODO: redo should be retrieved from the taskInstance
    process.setRedoCount(applicationConfiguration.taskConfiguration.getRetries());
    process.setStages(applicationConfiguration.processConfiguration.getStageArray());
    return process;
  }
}
