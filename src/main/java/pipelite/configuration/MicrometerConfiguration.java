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
package pipelite.configuration;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.micrometer.stackdriver.StackdriverConfig;
import io.micrometer.stackdriver.StackdriverMeterRegistry;
import java.time.Duration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.actuate.autoconfigure.metrics.MeterRegistryCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class MicrometerConfiguration {

  @Autowired private ServiceConfiguration serviceConfiguration;

  @Value("${management.metrics.export.stackdriver.projectId:#{null}}")
  private String stackDriverProjectId;

  @Value("${management.metrics.export.stackdriver.step:#{null}}")
  private Duration stackDriverStep;

  @Bean
  public MeterRegistry meterRegistry() {
    if (stackDriverProjectId != null) {
      final StackdriverConfig stackdriverConfig =
          new StackdriverConfig() {
            @Override
            public String projectId() {
              return stackDriverProjectId;
            }

            @Override
            public Duration step() {
              return (stackDriverStep != null) ? stackDriverStep : Duration.ofMinutes(5);
            }

            @Override
            public String get(final String key) {
              return null;
            }

            @Override
            public boolean useSemanticMetricTypes() {
              return true;
            }
          };

      return StackdriverMeterRegistry.builder(stackdriverConfig).build();
    }

    return new SimpleMeterRegistry();
  }

  @Bean
  public MeterRegistryCustomizer<MeterRegistry> customizeMeterRegistry() {
    return (registry) ->
        registry
            .config()
            .commonTags(
                "application",
                serviceConfiguration.getName(),
                "url",
                serviceConfiguration.getUrl());
  }
}
