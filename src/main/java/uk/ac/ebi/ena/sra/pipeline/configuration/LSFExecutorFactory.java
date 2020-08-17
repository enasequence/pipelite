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

import pipelite.configuration.LSFTaskExecutorConfiguration;
import pipelite.configuration.ProcessConfiguration;
import pipelite.configuration.TaskExecutorConfiguration;
import pipelite.resolver.ExceptionResolver;
import uk.ac.ebi.ena.sra.pipeline.launcher.LSFStageExecutor;
import uk.ac.ebi.ena.sra.pipeline.launcher.PipeliteLauncher.StageExecutorFactory;
import pipelite.task.executor.TaskExecutor;

public class LSFExecutorFactory implements StageExecutorFactory {
  private final String pipeline_name;
  private final ExceptionResolver resolver;
  private final ProcessConfiguration processConfiguration;
  private final TaskExecutorConfiguration taskExecutorConfiguration;
  private final LSFTaskExecutorConfiguration lsfTaskExecutorConfiguration;

  public LSFExecutorFactory(
      String pipeline_name,
      ExceptionResolver resolver,
      ProcessConfiguration processConfiguration,
      TaskExecutorConfiguration taskExecutorConfiguration,
      LSFTaskExecutorConfiguration lsfTaskExecutorConfiguration) {

    this.pipeline_name = pipeline_name;
    this.resolver = resolver;
    this.processConfiguration = processConfiguration;
    this.taskExecutorConfiguration = taskExecutorConfiguration;
    this.lsfTaskExecutorConfiguration = lsfTaskExecutorConfiguration;
  }

  public TaskExecutor create() {
    TaskExecutor executor =
        new LSFStageExecutor(
            pipeline_name,
            resolver,
            processConfiguration,
            taskExecutorConfiguration,
            lsfTaskExecutorConfiguration);

    return executor;
  }
}
