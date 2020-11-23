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

import java.net.InetAddress;
import java.time.Duration;
import java.util.concurrent.ForkJoinPool;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.extern.flogger.Flogger;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Flogger
@Data
@Builder
@AllArgsConstructor
@Configuration
@ConfigurationProperties(prefix = "pipelite.launcher")
/**
 * Pipelite supports two different launchers. The PipeliteLauncher executes processes in parallel
 * for one pipeline. The PipeliteScheduler executes non-parallel processes for one or more pipelines
 * with cron schedules.
 */
public class LauncherConfiguration {

  public static final Duration DEFAULT_PROCESS_LAUNCH_FREQUENCY = Duration.ofMinutes(1);
  public static final Duration DEFAULT_PROCESS_REFRESH_FREQUENCY = Duration.ofHours(1);
  public static final int DEFAULT_PIPELINE_PARALLELISM = ForkJoinPool.getCommonPoolParallelism();
  public static final Duration DEFAULT_STAGE_LAUNCH_FREQUENCY = Duration.ofMinutes(1);
  public static final Duration DEFAULT_STAGE_POLL_FREQUENCY = Duration.ofMinutes(1);

  public LauncherConfiguration() {}

  /**
   * The name of the PipeliteLauncher or PipeliteScheduler. Only one launcher with a given name can
   * be executed in parallel. Default PipeliteLauncher name is <host name>@<pipeline name>. Default
   * PipeliteScheduler name is <host name>@<user name>.
   */
  private String launcherName;

  /** The PipeliteLauncher will execute processes from this pipeline. */
  private String pipelineName;

  /**
   * The PipeliteLauncher will execute processes in parallel. The pipelineParallelism is the maximum
   * number of processes executed in parallel.
   */
  private int pipelineParallelism;

  /**
   * The PipeliteLauncher and PipeliteScheduler periodically execute new processes. The
   * processLaunchFrequency is the frequency of doing this.
   */
  private Duration processLaunchFrequency;

  /**
   * The PipeliteLauncher and PipeliteScheduler periodically refresh their process execution queue.
   * The processRefreshFrequency is the frequency of doing this.
   */
  private Duration processRefreshFrequency;

  /**
   * The PipeliteLauncher and PipeliteScheduler periodically executes new process stages. The
   * stageLaunchFrequency is the frequency of doing this.
   */
  private Duration stageLaunchFrequency;

  /**
   * The PipeliteLauncher and PipeliteScheduler periodically poll for stage execution results. The
   * stagePollFrequency is the frequency of doing this.
   */
  private Duration stagePollFrequency;

  /** The PipeliteLauncher can optionally be shut down if idle. */
  private boolean shutdownIfIdle;

  /** Defaults to <host name>@<pipeline name>. */
  public static String getLauncherNameForPipeliteLauncher(
      LauncherConfiguration launcherConfiguration, String pipelineName) {
    return launcherConfiguration.getLauncherName() != null
        ? launcherConfiguration.getLauncherName()
        : getHostName() + "@" + pipelineName;
  }

  /** Defaults to <host name>@<user name>. */
  public static String getLauncherNameForPipeliteScheduler(
      LauncherConfiguration launcherConfiguration) {
    return launcherConfiguration.getLauncherName() != null
        ? launcherConfiguration.getLauncherName()
        : getHostName() + "@" + getUserName();
  }

  public static String getHostName() {
    try {
      return InetAddress.getLocalHost().getHostName();
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  public static String getUserName() {
    try {
      return System.getProperty("user.name");
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }
}
