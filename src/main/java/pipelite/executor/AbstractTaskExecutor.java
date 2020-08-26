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

import java.util.*;

import pipelite.configuration.TaskConfigurationEx;
import pipelite.resolver.ExceptionResolver;
import pipelite.task.result.TaskExecutionResult;

public abstract class AbstractTaskExecutor implements TaskExecutor {
  protected final TaskConfigurationEx taskConfiguration;
  protected final ExceptionResolver resolver;
  protected final TaskExecutionResult internalError;

  public AbstractTaskExecutor(TaskConfigurationEx taskConfiguration) {
    this.taskConfiguration = taskConfiguration;
    this.resolver = taskConfiguration.getResolver();
    this.internalError = resolver.internalError();
  }

  public List<String> getEnvAsJavaSystemPropertyOptions() {
    List<String> options = new ArrayList<>();
    if (taskConfiguration.getEnv() != null) {
      for (String property : taskConfiguration.getEnv()) {
        String value = System.getProperty(property);
        if (value != null) {
          options.add(String.format("-D%s=%s", property, value));
        }
      }
    }
    return options;
  }
}
