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
package pipelite.stage.executor;

import java.time.Duration;
import java.util.Map;

/**
 * Default stage configuration parameters. Some parameters are only supported by specific executors.
 */
public interface ConfigurableStageExecutorParameters {

  Duration DEFAULT_TIMEOUT = Duration.ofDays(7);
  Duration DEFAULT_POLL_FREQUENCY = Duration.ofMinutes(1);
  int DEFAULT_MAX_RETRIES = 3;
  int DEFAULT_IMMEDIATE_RETRIES = 0;

  /**
   * Returns the execution timeout.
   *
   * @return the execution timeout
   */
  Duration getTimeout();

  /**
   * Returns the maximum number of retries.
   *
   * @return the maximum number of retries
   */
  Integer getMaximumRetries();

  /**
   * Returns the maximum number of immediate retries.
   *
   * @return the maximum number of immediate retries
   */
  Integer getImmediateRetries();

  /**
   * Returns the environmental variables.
   *
   * @return the environmental variables
   */
  Map<String, String> getEnv();

  /**
   * Returns the working directory.
   *
   * @return the working directory
   */
  String getWorkDir();

  /**
   * Returns the amount of requested memory in MBytes.
   *
   * @return the amount of requested memory in MBytes
   */
  Integer getMemory();

  Duration getMemoryTimeout();

  /**
   * Returns the number of requested cores.
   *
   * @return the number of requested cores
   */
  Integer getCores();

  /**
   * Returns the execution host for remote executors.
   *
   * @return the execution host for remote executors
   */
  String getHost();

  /**
   * Returns the queue name for queue based executors.
   *
   * @return the queue name for queue based executors
   */
  String getQueue();

  /**
   * Returns poll frequency for asynchronous executors.
   *
   * @return the poll frequency for asynchronous executors
   */
  Duration getPollFrequency();

  /**
   * Returns the singularity image name.
   *
   * @return the singularity image name
   */
  String getSingularityImage();
}
