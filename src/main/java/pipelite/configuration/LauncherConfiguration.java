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
import java.util.UUID;
import lombok.Data;
import lombok.extern.flogger.Flogger;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

/**
 * Configuration for {@link pipelite.launcher.PipeliteLauncher} and {@link
 * pipelite.launcher.PipeliteScheduler}. {@link pipelite.launcher.PipeliteLauncher} executes
 * processes in parallel for one pipeline. {@link pipelite.launcher.PipeliteScheduler} executes
 * non-parallel processes using cron schedules. Processes are created using {@link
 * pipelite.process.ProcessFactory}.
 */
@Flogger
@Data
@Configuration
@ConfigurationProperties(prefix = "pipelite.launcher")
public class LauncherConfiguration {

  private static final Duration DEFAULT_LOCK_DURATION = Duration.ofMinutes(10);
  private static final Duration DEFAULT_PROCESS_RUNNER_FREQUENCY = Duration.ofMinutes(1);
  private static final Duration DEFAULT_SCHEDULE_REFRESH_FREQUENCY = Duration.ofHours(4);
  private static final Duration DEFAULT_PROCESS_QUEUE_MAX_REFRESH_FREQUENCY = Duration.ofHours(6);
  private static final Duration DEFAULT_PROCESS_QUEUE_MIN_REFRESH_FREQUENCY = Duration.ofMinutes(5);
  private static final int DEFAULT_PROCESS_QUEUE_MAX_SIZE = 5000;
  private static final int DEFAULT_PROCESS_CREATE_MAX_SIZE = 5000;

  public LauncherConfiguration() {}

  /**
   * An optional comma separated list of {@link pipelite.launcher.PipeliteLauncher} pipeline names.
   * {@link pipelite.launcher.PipeliteLauncher} executes processes in parallel for one pipeline.
   */
  private String pipelineName;

  /**
   * An optional {@link pipelite.launcher.PipeliteScheduler} name. {@link
   * pipelite.launcher.PipeliteScheduler} executes non-parallel processes using cron schedules.
   */
  private String schedulerName;

  /**
   * The duration after which {@link pipelite.launcher.PipeliteLauncher} and {@link
   * pipelite.launcher.PipeliteScheduler} created process locks expire unless they are renewed.
   */
  private Duration lockDuration = DEFAULT_LOCK_DURATION;

  /**
   * The running frequency for {@link pipelite.launcher.PipeliteLauncher} and {@link
   * pipelite.launcher.PipeliteScheduler}.
   */
  private Duration processRunnerFrequency = DEFAULT_PROCESS_RUNNER_FREQUENCY;

  /**
   * The frequency for {@link pipelite.launcher.PipeliteScheduler} to refresh its process schedules.
   */
  private Duration scheduleRefreshFrequency = DEFAULT_SCHEDULE_REFRESH_FREQUENCY;

  /**
   * The maximum frequency for {@link pipelite.launcher.PipeliteLauncher} to refresh its process
   * execution queue.
   */
  private Duration processQueueMaxRefreshFrequency = DEFAULT_PROCESS_QUEUE_MAX_REFRESH_FREQUENCY;

  /**
   * The minimum frequency for {@link pipelite.launcher.PipeliteLauncher} to refresh its process
   * execution queue.
   */
  private Duration processQueueMinRefreshFrequency = DEFAULT_PROCESS_QUEUE_MIN_REFRESH_FREQUENCY;

  /**
   * The maximum length of {@link pipelite.launcher.PipeliteLauncher} process execution queue. The
   * queue will be refreshed if it becomes smaller than the pipeline parallelism.
   */
  private int processQueueMaxSize = DEFAULT_PROCESS_QUEUE_MAX_SIZE;

  /**
   * The maximum number of new processes created by {@link pipelite.launcher.PipeliteLauncher} using
   * using {@link pipelite.process.ProcessSource} before new processes are executed.
   */
  private int processCreateMaxSize = DEFAULT_PROCESS_CREATE_MAX_SIZE;

  /** The {@link pipelite.launcher.PipeliteLauncher} can optionally be shut down if idle. */
  private boolean shutdownIfIdle;

  public static String getLauncherName(String pipelineName, int port) {
    return pipelineName
        + "@"
        + WebConfiguration.getCanonicalHostName()
        + ":"
        + port
        + ":"
        + UUID.randomUUID();
  }

  public static String getSchedulerName(LauncherConfiguration launcherConfiguration) {
    return launcherConfiguration.getSchedulerName();
  }
}
