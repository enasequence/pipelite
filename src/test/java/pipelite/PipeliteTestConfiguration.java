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

import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.util.TestPropertyValues;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.FilterType;
import pipelite.configuration.WebServerCustomizer;

@EnableAutoConfiguration
@ComponentScan(
    basePackages = {
      "pipelite.service",
      "pipelite.launcher",
      "pipelite.repository",
      "pipelite.configuration",
      "pipelite.metrics"
    },
    excludeFilters =
        @ComponentScan.Filter(
            type = FilterType.ASSIGNABLE_TYPE,
            classes = {WebServerCustomizer.class}))
public class PipeliteTestConfiguration {

  /** Creates a unique service name for PipeliteLockerService. */
  public static class TestContextInitializer
      implements ApplicationContextInitializer<ConfigurableApplicationContext> {
    @Override
    public void initialize(ConfigurableApplicationContext configurableApplicationContext) {
      TestPropertyValues.of(
              "pipelite.service.name="
                  + UniqueStringGenerator.randomServiceName(PipeliteTestConfiguration.class),
              "pipelite.service.force=true")
          .applyTo(configurableApplicationContext.getEnvironment());
    }
  }
}
