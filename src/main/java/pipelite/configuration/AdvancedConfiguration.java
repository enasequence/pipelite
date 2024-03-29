/*
 * Copyright 2020-2022 EMBL - European Bioinformatics Institute
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
import lombok.Data;
import lombok.extern.flogger.Flogger;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import pipelite.runner.pipeline.PipelineRunner;
import pipelite.runner.process.ProcessQueuePriorityPolicy;
import pipelite.runner.schedule.ScheduleRunner;

/**
 * Advanced configuration for {@link PipelineRunner} and {@link ScheduleRunner}. {@link
 * PipelineRunner} executes processes in parallel for one pipeline. {@link ScheduleRunner} executes
 * non-parallel processes using cron schedules.
 */
@Flogger
@Data
@Configuration
@ConfigurationProperties(prefix = "pipelite.advanced")
public class AdvancedConfiguration {

  public static final Duration DEFAULT_LOCK_FREQUENCY = Duration.ofMinutes(5);
  public static final Duration DEFAULT_LOCK_DURATION = Duration.ofMinutes(60);
  private static final Duration DEFAULT_PROCESS_RUNNER_FREQUENCY = Duration.ofSeconds(1);
  private static final int DEFAULT_PROCESS_RUNNER_WORKERS = 25;
  private static final Duration DEFAULT_SCHEDULE_REFRESH_FREQUENCY = Duration.ofHours(4);

  private static final ProcessQueuePriorityPolicy DEFAULT_PROCESS_QUEUE_PRIORITY_POLICY =
      ProcessQueuePriorityPolicy.BALANCED;

  private static final Duration DEFAULT_PROCESS_QUEUE_MAX_REFRESH_FREQUENCY = Duration.ofHours(4);
  private static final Duration DEFAULT_PROCESS_QUEUE_MIN_REFRESH_FREQUENCY =
      Duration.ofMinutes(10);

  public static final int DEFAULT_MAIL_LOG_BYTES = 5 * 1024;

  public AdvancedConfiguration() {}

  /** The frequency of renewing service locks. */
  private Duration lockFrequency = DEFAULT_LOCK_FREQUENCY;

  /**
   * The duration after which service and process locks expire unless the service lock is renewed.
   */
  private Duration lockDuration = DEFAULT_LOCK_DURATION;

  /** The frequency for {@link pipelite.runner.process.ProcessRunnerPool}. */
  private Duration processRunnerFrequency = DEFAULT_PROCESS_RUNNER_FREQUENCY;

  /** The number of workers for process runners. */
  private int processRunnerWorkers = DEFAULT_PROCESS_RUNNER_WORKERS;

  /** The minimum refresh frequency of process queue in {@link PipelineRunner}. */
  private ProcessQueuePriorityPolicy processQueuePriorityPolicy =
      DEFAULT_PROCESS_QUEUE_PRIORITY_POLICY;

  /** The minimum refresh frequency of process queue in {@link PipelineRunner}. */
  private Duration processQueueMinRefreshFrequency = DEFAULT_PROCESS_QUEUE_MIN_REFRESH_FREQUENCY;

  /** The maximum refresh frequency of process queue in {@link PipelineRunner}. */
  private Duration processQueueMaxRefreshFrequency = DEFAULT_PROCESS_QUEUE_MAX_REFRESH_FREQUENCY;

  /** The {@link PipelineRunner} can be shut down if it is idle. */
  private boolean shutdownIfIdle;

  /** The maximum size of pipelite stage execution logs emailed. */
  private int mailLogBytes = DEFAULT_MAIL_LOG_BYTES;
}
