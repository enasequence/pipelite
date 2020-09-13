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
import pipelite.launcher.pipelite.PipeliteLauncher;
import pipelite.launcher.process.ProcessLauncher;
import pipelite.launcher.schedule.ScheduleLauncher;

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

  /** Frequency of assigning processes to workers in PipeliteLauncher. */
  @Builder.Default private Duration processLaunchFrequency = PipeliteLauncher.DEFAULT_PROCESS_LAUNCH_FREQUENCY;

  /** Frequency of assigning tasks to workers in ProcessLauncher. */
  @Builder.Default private Duration taskLaunchFrequency = ProcessLauncher.DEFAULT_TASK_LAUNCH_FREQUENCY;

  /** Delay between re-prioritising process executions in PipeliteLauncher. */
  @Builder.Default
  private Duration prioritizationFrequency = PipeliteLauncher.DEFAULT_PRIORITIZATION_FREQUENCY;

  /** Frequency of scheduling processes to execute in ScheduleLauncher. */
  @Builder.Default
  private Duration schedulingFrequency = ScheduleLauncher.DEFAULT_SCHEDULING_FREQUENCY;
}
