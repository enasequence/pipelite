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
package pipelite.configuration;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import pipelite.process.ProcessSource;

@Data
@Builder
@AllArgsConstructor
@Configuration
@ConfigurationProperties(prefix = "pipelite.process", ignoreInvalidFields = true)
public class ProcessConfiguration {

  public ProcessConfiguration() {}

  private String pipelineName;

  /**
   * Name of the ProcessSource class that creates new process ids to be executed. A Process is
   * uniquely identified by the process id. When being executed the ProcessFactory is used to create
   * the execution instructions for the Process given the process id.
   */
  private String processSourceClassName;

  private ProcessSource processSource;

  private ProcessSource getProcessSource() {
    return processSource;
  }

  public static ProcessSource getProcessSource(ProcessConfiguration processConfiguration) {
    if (processConfiguration.getProcessSource() != null) {
      return processConfiguration.getProcessSource();
    }
    if (processConfiguration.getProcessSourceClassName() != null) {
      try {
        Class<?> cls = Class.forName(processConfiguration.getProcessSourceClassName());
        if (ProcessSource.class.isAssignableFrom(cls)) {
          ProcessSource source = ((ProcessSource) cls.getDeclaredConstructor().newInstance());
          processConfiguration.setProcessSource(source);
          return source;
        }
      } catch (Exception ex) {
        throw new RuntimeException("Could not create process source", ex);
      }
    }
    throw new RuntimeException("Could not create process source");
  }

  public boolean isProcessSource() {
    return processSourceClassName != null || processSource != null;
  }
}
