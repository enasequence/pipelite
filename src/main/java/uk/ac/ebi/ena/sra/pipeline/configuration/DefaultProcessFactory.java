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

import pipelite.task.result.resolver.ExecutionResultExceptionResolver;
import uk.ac.ebi.ena.sra.pipeline.launcher.PipeliteLauncher.PipeliteProcess;
import uk.ac.ebi.ena.sra.pipeline.launcher.PipeliteLauncher.ProcessFactory;
import uk.ac.ebi.ena.sra.pipeline.launcher.PipeliteLauncher.StageExecutorFactory;
import uk.ac.ebi.ena.sra.pipeline.launcher.ProcessLauncher;

public class DefaultProcessFactory implements ProcessFactory {

  private final ExecutionResultExceptionResolver resolver;

  public DefaultProcessFactory(ExecutionResultExceptionResolver resolver) {
    this.resolver = resolver;
  }

  @Override
  public PipeliteProcess getProcess(String process_id) {
    ProcessLauncher process = new ProcessLauncher(resolver);
    process.setPipelineName(DefaultConfiguration.currentSet().getPipelineName());
    process.setProcessID(process_id);
    process.setRedoCount(DefaultConfiguration.currentSet().getStagesRedoCount());
    // TODO: locking routine should be changed
    // process.setExecutor( new DetachedStageExecutor(
    // DefaultConfiguration.currentSet().getCommitStatus() ) );

    process.setStages(DefaultConfiguration.currentSet().getStages());
    return process;
  }
}
