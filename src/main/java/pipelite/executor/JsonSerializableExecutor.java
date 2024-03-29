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
package pipelite.executor;

import pipelite.json.Json;
import pipelite.stage.executor.StageExecutor;

/** Marker interface to make the executor serialized as json. */
public interface JsonSerializableExecutor {

  /** Serializes the executor to json. */
  default String serialize() {
    return Json.serialize(this);
  }

  /**
   * Deserializes the executor from json.
   *
   * @param className the name of the executor class
   * @param json the json
   * @return the executor
   */
  static StageExecutor deserialize(String className, String json) {
    return Json.deserialize(json, className, StageExecutor.class);
  }
}
