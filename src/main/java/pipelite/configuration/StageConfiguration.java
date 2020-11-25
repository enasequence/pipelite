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
import lombok.Builder;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import pipelite.executor.ConfigurableStageExecutorParameters;

@Data
@Builder
@Configuration
@ConfigurationProperties(prefix = "pipelite.stage", ignoreInvalidFields = true)
/** Some configuration parameters are supported only by specific executors. */
public class StageConfiguration implements ConfigurableStageExecutorParameters {

  public StageConfiguration() {}

  public StageConfiguration(
      String host,
      Duration timeout,
      Integer memory,
      Duration memoryTimeout,
      Integer cores,
      String queue,
      Integer maximumRetries,
      Integer immediateRetries,
      String workDir,
      Map<String, String> env,
      String singularityImage) {
    this.host = host;
    this.timeout = timeout != null ? timeout : DEFAULT_TIMEOUT;
    this.memory = memory;
    this.memoryTimeout = memoryTimeout;
    this.cores = cores;
    this.queue = queue;
    this.maximumRetries = maximumRetries != null ? maximumRetries : DEFAULT_MAX_RETRIES;
    this.immediateRetries = immediateRetries;
    this.workDir = workDir;
    this.env = env;
    this.singularityImage = singularityImage;
  }

  /** Remote host. */
  private String host;

  /** Execution timeout. */
  @Builder.Default private Duration timeout = DEFAULT_TIMEOUT;

  /** Memory reservation (MBytes). */
  private Integer memory;

  /** Memory reservation timeout (minutes). */
  private Duration memoryTimeout;

  /** Core reservation. */
  private Integer cores;

  /** Queue name. */
  private String queue;

  /** Number of maximum retries. */
  @Builder.Default private Integer maximumRetries = DEFAULT_MAX_RETRIES;

  /** Number of maximum retries. */
  private Integer immediateRetries;

  /** Work directory. */
  private String workDir;

  /** Environmental variables. */
  @Builder.Default private Map<String, String> env = new HashMap<>();

  private String singularityImage;
}
