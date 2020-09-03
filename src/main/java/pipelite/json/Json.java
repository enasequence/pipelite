package pipelite.json;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import lombok.extern.flogger.Flogger;
import pipelite.executor.TaskExecutor;

@Flogger
public class Json {

  private Json() {}

  private static final ObjectMapper mapper = new ObjectMapper();

  static {
    mapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
    mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
  }

  public static String serializeNullIfError(Object object) {
    try {
      return mapper.writerWithDefaultPrettyPrinter().writeValueAsString(object);
    } catch (Exception ex) {
      log.atSevere().withCause(ex).log("Failed to serialize json");
      return null;
    }
  }

  public static String serializeThrowIfError(Object object) {
    try {
      return mapper.writeValueAsString(object);
    } catch (Exception ex) {
      log.atSevere().withCause(ex).log("Failed to serialize json");
      throw new RuntimeException(ex);
    }
  }

  public static TaskExecutor deserializeNullIfError(String json, String className) {
    try {
      return (TaskExecutor) mapper.readValue(json, Class.forName(className));
    } catch (Exception ex) {
      log.atSevere().withCause(ex).log("Failed to deserialize json");
      return null;
    }
  }

  public static TaskExecutor deserializeThrowIfError(String json, String className) {
    try {
      return (TaskExecutor) mapper.readValue(json, Class.forName(className));
    } catch (Exception ex) {
      log.atSevere().withCause(ex).log("Failed to deserialize json");
      throw new RuntimeException(ex);
    }
  }
}
