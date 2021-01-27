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

import com.google.common.base.Supplier;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;
import lombok.extern.flogger.Flogger;
import pipelite.configuration.ExecutorConfiguration;
import pipelite.exception.PipeliteException;
import pipelite.json.Json;

/** Parameters shared by all executors. */
@Data
@NoArgsConstructor
@SuperBuilder
@Flogger
public class ExecutorParameters {

  public static final Duration DEFAULT_TIMEOUT = Duration.ofDays(7);
  public static final int DEFAULT_MAX_RETRIES = 0;
  public static final int DEFAULT_IMMEDIATE_RETRIES = 0;

  /** The execution timeout. */
  @Builder.Default private Duration timeout = DEFAULT_TIMEOUT;

  /** The maximum number of retries */
  @Builder.Default private Integer maximumRetries = DEFAULT_MAX_RETRIES;

  /** The maximum number of immediate retries. */
  @Builder.Default private Integer immediateRetries = DEFAULT_IMMEDIATE_RETRIES;

  public static <T> void applyDefault(
      Supplier<T> thisGetter, Consumer<T> thisSetter, Supplier<T> defaultGetter) {
    if (thisGetter.get() == null) {
      thisSetter.accept(defaultGetter.get());
    }
  }

  public static <K, V> void applyMapDefaults(Map<K, V> params, Map<K, V> defaultParams) {
    if (params == null) {
      params = new HashMap<>();
    }
    if (defaultParams != null) {
      for (K key : defaultParams.keySet()) {
        if (!params.containsKey(key)) {
          params.put(key, defaultParams.get(key));
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
  }

  /** Validates the parameters after applying defaults. */
  public void validate() {
    validateNotNull(timeout, "timeout");
    validateNotNull(maximumRetries, "maximumRetries");
    validateNotNull(immediateRetries, "immediateRetries");
  }

  protected static <T> T validateNotNull(T value, String name) {
    if (value == null) {
      throw new PipeliteException("Missing " + name + " parameter");
    }
    return value;
  }

  public static Path validatePath(String path, String name) {
    try {
      return Paths.get(path).normalize();
    } catch (Exception e) {
      throw new PipeliteException("Invalid " + name + " parameter path: " + path);
    }
  }

  public static URL validateUrl(String url, String name) {
    validateNotNull(url, name);
    try {
      // Try to create a URL.
      new URL(url).openConnection().connect();
      return new URL(url);
    } catch (Exception e1) {
      // Try to create a resource URL.
      try {
        // Remove leading '/'.
        url = url.replaceFirst("^/+", "");
        URL resourceUrl = ExecutorParameters.class.getClassLoader().getResource(url);
        if (resourceUrl == null) {
          throw new PipeliteException("Invalid " + name + " parameter url: " + url);
        }
        return resourceUrl;
      } catch (Exception e2) {
        throw new PipeliteException("Invalid " + name + " parameter url: " + url);
      }
    }
  }

  /** Serializes the executor to json. */
  public String serialize() {
    return Json.serialize(this);
  }

  /** Deserializes the executor from json. */
  public static <T extends ExecutorParameters> T deserialize(String json, Class<T> cls) {
    return Json.deserialize(json, cls);
  }

  @Override
  public String toString() {
    return serialize();
  }
}
