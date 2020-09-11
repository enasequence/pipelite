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
import pipelite.launcher.PipeliteLauncher;

@Data
@Builder
@AllArgsConstructor
@ConfigurationProperties(prefix = "pipelite.launcher")
public class LauncherConfiguration {

  public LauncherConfiguration() {}

  /** Unique launcher name. */
  private String launcherName;

  /** Number of maximum parallel process executions in PipeliteLauncher and ScheduleLauncher. */
  @Builder.Default private int workers = PipeliteLauncher.DEFAULT_WORKERS;

  /**
   * Delay between assigning all workers with processes to execute in PipeliteLauncher and
   * ScheduleLauncher.
   */
  @Builder.Default private Duration runDelay = PipeliteLauncher.DEFAULT_RUN_DELAY;

  /** Delay between re-prioritising process executions in PipeliteLauncher. */
  @Builder.Default
  private Duration prioritizationDelay = PipeliteLauncher.DEFAULT_PRIORITIZATION_DELAY;
}
