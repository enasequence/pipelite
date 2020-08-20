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
package pipelite.executor;

import lombok.Value;
import pipelite.configuration.ProcessConfiguration;
import pipelite.configuration.TaskConfiguration;
import pipelite.resolver.ExceptionResolver;
import uk.ac.ebi.ena.sra.pipeline.launcher.LSFTaskExecutor;

@Value
public class LsfTaskExecutorFactory implements TaskExecutorFactory {
  private final ProcessConfiguration processConfiguration;
  private final TaskConfiguration taskConfiguration;

  @Override
  public TaskExecutor create() {
    TaskExecutor executor = new LSFTaskExecutor(processConfiguration, taskConfiguration);

    return executor;
  }
}