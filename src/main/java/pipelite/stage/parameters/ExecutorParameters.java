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
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;
import lombok.extern.flogger.Flogger;
import pipelite.configuration.StageConfiguration;
import pipelite.exception.PipeliteException;
import pipelite.executor.AwsBatchExecutor;
import pipelite.executor.CmdExecutor;
import pipelite.executor.LsfExecutor;
import pipelite.json.Json;
import pipelite.stage.executor.StageExecutor;

import java.time.Duration;
import java.util.function.Consumer;

/** Parameters shared by all executors. */
@Data
@NoArgsConstructor
@SuperBuilder
@Flogger
public class ExecutorParameters {

  public static final Duration DEFAULT_TIMEOUT = Duration.ofDays(7);
  public static final int DEFAULT_MAX_RETRIES = 3;
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

  /** Override to apply default values from stage configuration.
   *
   * @param stageConfiguration the stage configuration
   */
  public void applyDefaults(StageConfiguration stageConfiguration) {}

  /**
   * Call to apply default values from stage configuration.
   *
   * @param params executor parameters extracted from stage configuration
   */
  protected void applyDefaults(ExecutorParameters params) {
    applyDefault(this::getTimeout, this::setTimeout, params::getTimeout);
    applyDefault(this::getMaximumRetries, this::setMaximumRetries, params::getMaximumRetries);
    applyDefault(this::getImmediateRetries, this::setImmediateRetries, params::getImmediateRetries);
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
