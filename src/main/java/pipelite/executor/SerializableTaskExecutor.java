package pipelite.executor;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

public interface SerializableTaskExecutor extends TaskExecutor {

  default String serialize() {
    ObjectMapper objectMapper = new ObjectMapper();
    objectMapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
    try {
      return objectMapper.writeValueAsString(this);
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  static TaskExecutor deserialize(String className, String data) {
    ObjectMapper objectMapper = new ObjectMapper();
    objectMapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
    try {
      return (TaskExecutor) objectMapper.readValue(data, Class.forName(className));
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }
}
