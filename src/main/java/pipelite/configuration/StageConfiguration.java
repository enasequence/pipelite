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

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import pipelite.stage.executor.ConfigurableStageExecutorParameters;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Configuration
@ConfigurationProperties(prefix = "pipelite.stage", ignoreInvalidFields = true)
public class StageConfiguration implements ConfigurableStageExecutorParameters {

  @Builder.Default private Duration timeout = DEFAULT_TIMEOUT;

  @Builder.Default private Integer maximumRetries = DEFAULT_MAX_RETRIES;

  @Builder.Default private Integer immediateRetries = DEFAULT_IMMEDIATE_RETRIES;

  @Builder.Default private Map<String, String> env = new HashMap<>();

  private String workDir;

  private Integer memory;

  private Duration memoryTimeout;

  private Integer cores;

  private String host;

  private String queue;

  @Builder.Default private Duration pollFrequency = DEFAULT_POLL_FREQUENCY;

  private String singularityImage;
}
