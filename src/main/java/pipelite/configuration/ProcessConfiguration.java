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
import pipelite.process.ProcessFactory;
import pipelite.process.ProcessSource;

@Data
@Builder
@AllArgsConstructor
@Configuration
@ConfigurationProperties(prefix = "pipelite.process", ignoreInvalidFields = true)
public class ProcessConfiguration {

  public ProcessConfiguration() {}

  /** Name of the process begin executed. */
  private String pipelineName;

  /**
   * Name of the the class that creates process instances. Must implement the ProjectFactory
   * interface.
   */
  private String processFactoryName;

  private String processSourceName;

  private ProcessFactory processFactory;

  private ProcessFactory getProcessFactory() {
    return processFactory;
  }

  private ProcessSource processSource;

  private ProcessSource getProcessSource() {
    return processSource;
  }

  public static ProcessFactory getProcessFactory(ProcessConfiguration processConfiguration) {
    if (processConfiguration.getProcessFactory() != null) {
      return processConfiguration.getProcessFactory();
    }
    if (processConfiguration.getProcessFactoryName() != null) {
      ProcessFactory factory =
          ProcessFactory.getProcessFactory(processConfiguration.getProcessFactoryName());
      if (factory != null) {
        processConfiguration.setProcessFactory(factory);
        return factory;
      }
    }
    throw new RuntimeException("Could not create process factory");
  }

  public static ProcessSource getProcessSource(ProcessConfiguration processConfiguration) {
    if (processConfiguration.getProcessSource() != null) {
      return processConfiguration.getProcessSource();
    }
    if (processConfiguration.getProcessSourceName() != null) {
      try {
        Class<?> cls = Class.forName(processConfiguration.getProcessSourceName());
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
    return processSourceName != null || processSource != null;
  }
}
