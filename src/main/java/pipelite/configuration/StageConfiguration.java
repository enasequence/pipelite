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
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import pipelite.stage.ConfigurableStageParameters;

@Data
@Builder
@AllArgsConstructor
@ConfigurationProperties(prefix = "pipelite.stage", ignoreInvalidFields = true)
/** Some configuration parameters are supported only by specific executors. */
public class StageConfiguration implements ConfigurableStageParameters {

  public StageConfiguration() {}

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

  /** Number of retries. */
  @Builder.Default private Integer retries = DEFAULT_RETRIES;

  /** Work directory. */
  private String workDir;

  /** Environmental variables. */
  private String[] env;

  /** Delay between stage polls. */
  @Builder.Default private Duration pollDelay = DEFAULT_POLL_DELAY;

  private String singularityImage;
}
