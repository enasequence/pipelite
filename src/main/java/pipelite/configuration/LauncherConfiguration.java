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
import javax.annotation.PostConstruct;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.extern.flogger.Flogger;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Flogger
@Data
@AllArgsConstructor
@Configuration
@ConfigurationProperties(prefix = "pipelite.launcher")
/**
 * Pipelite supports two different launchers. The PipeliteLauncher executes processes in parallel
 * for one pipeline. The PipeliteScheduler executes non-parallel processes for one or more pipelines
 * with cron schedules.
 */
public class LauncherConfiguration {

  public static final Duration DEFAULT_PIPELINE_LOCK_DURATION = Duration.ofMinutes(10);
  public static final Duration DEFAULT_PIPELINE_UNLOCK_FREQUENCY = Duration.ofMinutes(30);
  public static final Duration DEFAULT_PROCESS_LAUNCH_FREQUENCY = Duration.ofMinutes(1);
  public static final Duration DEFAULT_PROCESS_REFRESH_FREQUENCY = Duration.ofHours(1);
  public static final int DEFAULT_PIPELINE_PARALLELISM = ForkJoinPool.getCommonPoolParallelism();
  public static final Duration DEFAULT_STAGE_LAUNCH_FREQUENCY = Duration.ofMinutes(1);
  public static final Duration DEFAULT_STAGE_POLL_FREQUENCY = Duration.ofMinutes(1);

  public LauncherConfiguration() {}

  @PostConstruct
  private void checkRequiredProperties() {
    boolean isValid = true;
    if (port == null) {
      log.atSevere().log("Missing required pipelite property: pipelite.launcher.port");
      isValid = false;
    }
    if (contextPath == null) {
      log.atSevere().log("Missing required pipelite property: pipelite.launcher.path");
      isValid = false;
    }
    if (!isValid) {
      throw new IllegalArgumentException(
          "Missing required pipelite properties: pipelite.launcher.*");
    }
  }

  /** The pipelite web server port number. */
  private Integer port = 8082;

  /** The pipelite web server context path. */
  private String contextPath = "/pipelite";

  /** The name of the PipeliteScheduler. The name must be unique. */
  private String schedulerName;

  /**
   * PipeliteLaunchers will execute processes from these pipelines. The pipelineName is a comma
   * separated list.
   */
  private String pipelineName;

  /**
   * The PipeliteLauncher will execute processes in parallel. The pipelineParallelism is the maximum
   * number of processes executed in parallel.
   */
  private int pipelineParallelism;

  /**
   * The PipeliteLauncher and PipeliteScheduler lock processes for execution. The
   * pipelineLockDuration is the duration after which locks expire unless they are renewed.
   */
  private Duration pipelineLockDuration;

  /**
   * The PipeliteLauncher and PipeliteScheduler lock processes for execution. The
   * pipelineUnlockFrequency is the frequency of removing expired locks.
   */
  private Duration pipelineUnlockFrequency;

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
  public static String getLauncherName(String pipelineName, int port) {
    return getCanonicalHostName() + ":" + port + "@" + pipelineName;
  }

  /** Defaults to <host name>@<user name>. */
  public static String getSchedulerName(LauncherConfiguration launcherConfiguration) {
    return launcherConfiguration.getSchedulerName();
  }

  public static String getCanonicalHostName() {
    try {
      return InetAddress.getLocalHost().getCanonicalHostName();
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }
}
