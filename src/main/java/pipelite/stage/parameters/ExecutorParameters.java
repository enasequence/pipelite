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
package pipelite.stage.parameters;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.Singular;
import lombok.experimental.SuperBuilder;
import lombok.extern.flogger.Flogger;
import pipelite.configuration.ExecutorConfiguration;
import pipelite.json.Json;
import pipelite.stage.parameters.cmd.LogFileSavePolicy;

/** Parameters shared by all executors. */
@Data
@NoArgsConstructor
@SuperBuilder
@Flogger
public class ExecutorParameters {

  public static final Duration DEFAULT_TIMEOUT = Duration.ofDays(7);
  public static final int DEFAULT_MAX_RETRIES = 3;
  public static final int DEFAULT_IMMEDIATE_RETRIES = 3;
  public static final LogFileSavePolicy DEFAULT_LOG_SAVE = LogFileSavePolicy.ERROR;
  public static final int DEFAULT_LOG_LINES = 1000;

  /** The execution timeout. */
  private Duration timeout;

  /** The maximum number of retries */
  private Integer maximumRetries;

  /** The maximum number of immediate retries. */
  private Integer immediateRetries;

  /** The permanent error exit codes. Permanent errors are never retried. */
  @Singular protected List<Integer> permanentErrors;

  /** The stage log file save policy in the database. */
  private LogFileSavePolicy logSave;

  /** The number of last lines from the stage log file saved in the database. */
  private Integer logLines;

  public Duration getTimeout() {
    return timeout == null ? DEFAULT_TIMEOUT : timeout;
  }

  public Integer getMaximumRetries() {
    return maximumRetries == null ? DEFAULT_MAX_RETRIES : maximumRetries;
  }

  public Integer getImmediateRetries() {
    return immediateRetries == null ? DEFAULT_IMMEDIATE_RETRIES : immediateRetries;
  }

  public List<Integer> getPermanentErrors() {
    return permanentErrors;
  }

  public LogFileSavePolicy getLogSave() {
    return logSave == null ? DEFAULT_LOG_SAVE : logSave;
  }

  public int getLogLines() {
    return (logLines == null || logLines < 1) ? DEFAULT_LOG_LINES : logLines;
  }

  public static <K, V> void applyMapDefaults(Map<K, V> map, Map<K, V> defaultMap) {
    if (defaultMap != null) {
      for (K key : defaultMap.keySet()) {
        if (!map.containsKey(key)) {
          map.put(key, defaultMap.get(key));
        }
      }
    }
  }

  public static <V> void applyListDefaults(List<V> list, List<V> defaultList) {
    if (defaultList != null) {
      for (V defaultValue : defaultList) {
        if (!list.contains(defaultValue)) {
          list.add(defaultValue);
        }
      }
    }
  }

  /**
   * Override to apply default values from stage configuration.
   *
   * @param executorConfiguration the stage configuration
   */
  public void applyDefaults(ExecutorConfiguration executorConfiguration) {}

  /**
   * Call to apply default values from stage configuration file.
   *
   * @param defaultParams executor parameters from configuration file
   */
  public void applyExecutorDefaults(ExecutorParameters defaultParams) {
    if (defaultParams == null) {
      return;
    }
    if (timeout == null) setTimeout(defaultParams.getTimeout());
    if (maximumRetries == null) setMaximumRetries(defaultParams.getMaximumRetries());
    if (immediateRetries == null) setImmediateRetries(defaultParams.getImmediateRetries());
    if (logSave == null) setLogSave(defaultParams.getLogSave());
    if (logLines == null) setLogLines(defaultParams.getLogLines());

    if (permanentErrors == null) {
      permanentErrors = new ArrayList<>();
    }
    applyListDefaults(permanentErrors, defaultParams.permanentErrors);
  }

  /** Validates the parameters after applying defaults. */
  public void validate() {}

  /** Serializes the executor to json. */
  public String serialize() {
    return Json.serialize(this);
  }

  /** Deserializes the executor from json. */
  public static <T extends ExecutorParameters> T deserialize(String json, Class<T> cls) {
    return Json.deserialize(json, cls);
  }

  // Json deserialization backwards compatibility for logBytes field.
  public void setLogBytes(int logBytes) {
    this.logLines = Math.max(logBytes / 100, 1);
  }

  @Override
  public String toString() {
    return serialize();
  }
}
