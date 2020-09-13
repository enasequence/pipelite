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
package pipelite.task;

import java.time.Duration;

/**
 * Task parameters that can be configured using TaskConfiguration. If the TaskInstance does not have
 * a TaskParameter value then a value from TaskConfiguration will be used.
 */
public interface ConfigurableTaskParameters {

  Duration DEFAULT_TIMEOUT = Duration.ofDays(7);
  int DEFAULT_RETRIES = 3;
  Duration DEFAULT_POLL_DELAY = Duration.ofMinutes(1);

  String getHost();

  Duration getTimeout();

  Integer getRetries();

  String[] getEnv();

  String getWorkDir();

  Integer getMemory();

  Duration getMemoryTimeout();

  Integer getCores();

  String getQueue();

  Duration getPollDelay();
}
