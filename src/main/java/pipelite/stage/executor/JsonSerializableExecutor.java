package pipelite.stage.executor;

import pipelite.json.Json;

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
