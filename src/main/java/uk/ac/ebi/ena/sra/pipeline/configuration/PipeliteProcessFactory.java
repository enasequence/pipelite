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
import pipelite.lock.ProcessInstanceLocker;
import pipelite.resolver.ExceptionResolver;
import uk.ac.ebi.ena.sra.pipeline.launcher.PipeliteLauncher.PipeliteProcess;
import uk.ac.ebi.ena.sra.pipeline.launcher.PipeliteLauncher.ProcessFactory;
import uk.ac.ebi.ena.sra.pipeline.launcher.ProcessLauncher;

@Value
public class PipeliteProcessFactory implements ProcessFactory {

  private final String launcherName;
  private final ApplicationConfiguration applicationConfiguration;
  private final ProcessInstanceLocker locker;

  @Override
  public PipeliteProcess create(String processId) {
    ExceptionResolver resolver = applicationConfiguration.processConfiguration.createResolver();

    ProcessLauncher process = new ProcessLauncher(launcherName, resolver, locker);
    process.setProcessID(processId);
    process.setPipelineName(applicationConfiguration.launcherConfiguration.getProcessName());
    process.setRedoCount(applicationConfiguration.taskExecutorConfiguration.getRetries());
    process.setStages(applicationConfiguration.processConfiguration.getStageArray());
    return process;
  }
}
