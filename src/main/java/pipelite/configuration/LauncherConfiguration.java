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
import java.util.UUID;
import java.util.concurrent.ForkJoinPool;
import javax.annotation.PostConstruct;
import lombok.Data;
import lombok.extern.flogger.Flogger;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Flogger
@Data
@Configuration
@ConfigurationProperties(prefix = "pipelite.launcher")
/**
 * Pipelite supports two different launchers. The PipeliteLauncher executes processes in parallel
 * for one pipeline. The PipeliteScheduler executes non-parallel processes for one or more pipelines
 * with cron schedules.
 */
public class LauncherConfiguration {

  private static final Duration DEFAULT_PIPELINE_LOCK_DURATION = Duration.ofMinutes(10);
  private static final Duration DEFAULT_PIPELINE_UNLOCK_FREQUENCY = Duration.ofMinutes(30);
  private static final Duration DEFAULT_PROCESS_LAUNCH_FREQUENCY = Duration.ofMinutes(1);
  private static final Duration DEFAULT_PROCESS_REFRESH_FREQUENCY = Duration.ofHours(1);
  private static final int DEFAULT_PIPELINE_PARALLELISM = ForkJoinPool.getCommonPoolParallelism();
  private static final Duration DEFAULT_STAGE_LAUNCH_FREQUENCY = Duration.ofMinutes(1);
  private static final Duration DEFAULT_STAGE_POLL_FREQUENCY = Duration.ofMinutes(1);

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

  /**
   * The PipeliteLauncher will execute processes from pipelines given process ids. The pipelineName
   * is a comma separated list of zero or more pipeline names.
   */
  private String pipelineName;

  /**
   * The PipeliteScheduler will execute processes given cron expressions. The schedulerName is the
   * name of an optional scheduler and must be unique.
   */
  private String schedulerName;

  /**
   * The PipeliteUnlocker will periodically remove expired locks. The unlockerName is the name of an
   * optional unlocker and must be unique.
   */
  private String unlockerName;

  /**
   * The PipeliteLauncher will execute processes in parallel. The pipelineParallelism is the maximum
   * number of processes executed in parallel.
   */
  private int pipelineParallelism = DEFAULT_PIPELINE_PARALLELISM;

  /**
   * The PipeliteLauncher and PipeliteScheduler lock processes for execution. The
   * pipelineLockDuration is the duration after which locks expire unless they are renewed.
   */
  private Duration pipelineLockDuration = DEFAULT_PIPELINE_LOCK_DURATION;

  /**
   * The PipeliteLauncher and PipeliteScheduler lock processes for execution. The
   * pipelineUnlockFrequency is the frequency of removing expired locks.
   */
  private Duration pipelineUnlockFrequency = DEFAULT_PIPELINE_UNLOCK_FREQUENCY;

  /**
   * The PipeliteLauncher and PipeliteScheduler periodically execute new processes. The
   * processLaunchFrequency is the frequency of doing this.
   */
  private Duration processLaunchFrequency = DEFAULT_PROCESS_LAUNCH_FREQUENCY;

  /**
   * The PipeliteLauncher and PipeliteScheduler periodically refresh their process execution queue.
   * The processRefreshFrequency is the frequency of doing this.
   */
  private Duration processRefreshFrequency = DEFAULT_PROCESS_REFRESH_FREQUENCY;

  /**
   * The ProcessLauncher periodically executes new process stages. The stageLaunchFrequency is the
   * frequency of doing this.
   */
  private Duration stageLaunchFrequency = DEFAULT_STAGE_LAUNCH_FREQUENCY;

  /**
   * The StageLauncher periodically poll for stage execution results. The stagePollFrequency is the
   * frequency of doing this.
   */
  private Duration stagePollFrequency = DEFAULT_STAGE_POLL_FREQUENCY;

  /** The PipeliteLauncher can optionally be shut down if idle. */
  private boolean shutdownIfIdle;

  public static String getLauncherName(String pipelineName, int port) {
    return pipelineName + "@" + getCanonicalHostName() + ":" + port + ":" + UUID.randomUUID();
  }

  public static String getSchedulerName(LauncherConfiguration launcherConfiguration) {
    return launcherConfiguration.getSchedulerName();
  }

  public static String getUnlockerName(LauncherConfiguration launcherConfiguration) {
    return launcherConfiguration.getUnlockerName();
  }

  public static String getCanonicalHostName() {
    try {
      return InetAddress.getLocalHost().getCanonicalHostName();
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }
}
