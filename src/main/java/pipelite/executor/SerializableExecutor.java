package pipelite.executor;

import pipelite.json.Json;

public interface SerializableExecutor extends TaskExecutor {

  default String serialize() {
    return Json.serializeThrowIfError(this);
  }

  static TaskExecutor deserialize(String className, String json) {
    return Json.deserializeThrowIfError(json, className);
  }
}
