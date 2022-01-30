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
package pipelite.stage.parameters;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Supplier;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.Singular;
import lombok.experimental.SuperBuilder;
import lombok.extern.flogger.Flogger;
import pipelite.configuration.ExecutorConfiguration;
import pipelite.json.Json;
import pipelite.stage.parameters.cmd.LogFileRetentionPolicy;
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
  public static final int DEFAULT_LOG_LINES = 1000;
  public static final Duration DEFAULT_LOG_TIMEOUT = Duration.ofSeconds(10);

  /** The execution timeout. */
  @Builder.Default private Duration timeout = DEFAULT_TIMEOUT;

  /** The maximum number of retries */
  @Builder.Default private Integer maximumRetries = DEFAULT_MAX_RETRIES;

  /** The maximum number of immediate retries. */
  @Builder.Default private Integer immediateRetries = DEFAULT_IMMEDIATE_RETRIES;

  /** The permanent error exit codes. Permanent errors are never retried. */
  @Singular protected List<Integer> permanentErrors;

  /** The stage log file save policy in the database. */
  private LogFileSavePolicy logSave;

  /** The stage log file retention policy in the working directory. */
  private LogFileRetentionPolicy logRetention;

  /** The number of last lines from the stage log file saved in the database. */
  @Builder.Default private int logLines = DEFAULT_LOG_LINES;

  /** The maximum wait time for the stage log file to become available. */
  @Builder.Default private Duration logTimeout = DEFAULT_LOG_TIMEOUT;

  public static <T> void applyDefault(
      Supplier<T> thisGetter, Consumer<T> thisSetter, Supplier<T> defaultGetter) {
    if (thisGetter.get() == null) {
      thisSetter.accept(defaultGetter.get());
    }
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
   * Call to apply default values from stage configuration.
   *
   * @param params executor parameters extracted from stage configuration
   */
  protected void applyDefaults(ExecutorParameters params) {
    if (params == null) {
      return;
    }
    applyDefault(this::getTimeout, this::setTimeout, params::getTimeout);
    applyDefault(this::getMaximumRetries, this::setMaximumRetries, params::getMaximumRetries);
    applyDefault(this::getImmediateRetries, this::setImmediateRetries, params::getImmediateRetries);
    applyDefault(this::getLogSave, this::setLogSave, params::getLogSave);
    applyDefault(this::getLogRetention, this::setLogRetention, params::getLogRetention);
    applyDefault(this::getLogLines, this::setLogLines, params::getLogLines);
    applyDefault(this::getLogTimeout, this::setLogTimeout, params::getLogTimeout);

    if (permanentErrors == null) {
      permanentErrors = new ArrayList<>();
    }
    applyListDefaults(permanentErrors, params.permanentErrors);
  }

  /** Validates the parameters after applying defaults. */
  public void validate() {
    ExecutorParametersValidator.validateNotNull(timeout, "timeout");
    ExecutorParametersValidator.validateNotNull(maximumRetries, "maximumRetries");
    ExecutorParametersValidator.validateNotNull(immediateRetries, "immediateRetries");
  }

  /** Serializes the executor to json. */
  public String serialize() {
    return Json.serialize(this);
  }

  /** Deserializes the executor from json. */
  public static <T extends ExecutorParameters> T deserialize(String json, Class<T> cls) {
    return Json.deserialize(json, cls);
  }

  public int getLogLines() {
    return (logLines < 1) ? DEFAULT_LOG_LINES : logLines;
  }

  @Override
  public String toString() {
    return serialize();
  }
}
