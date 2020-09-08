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

  /** Name of the pipelite.launcher begin executed. */
  private String launcherName;

  /** Number of parallel task executions. */
  @Builder.Default private int workers = PipeliteLauncher.DEFAULT_WORKERS;

  /** Delay between launcher runs. */
  @Builder.Default private Duration runDelay = PipeliteLauncher.DEFAULT_RUN_DELAY;

  /** Delay before launcher stops. */
  @Builder.Default private Duration stopDelay = PipeliteLauncher.DEFAULT_STOP_DELAY;

  /** Delay before launcher allows new high priority tasks to take precedence. */
  @Builder.Default private Duration priorityDelay = PipeliteLauncher.DEFAULT_PRIORITY_DELAY;
}
