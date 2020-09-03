package pipelite.json;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import lombok.extern.flogger.Flogger;
import pipelite.executor.TaskExecutor;

import java.util.regex.Pattern;

@Flogger
public class Json {

  private Json() {}

  private static final Pattern EMPTY_JSON = Pattern.compile("^\\s*\\{\\s*\\}\\s*$");
  private static final ObjectMapper mapper = new ObjectMapper();

  static {
    mapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
    mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
    mapper.setSerializationInclusion(JsonInclude.Include.NON_EMPTY);
  }

  private static String nullIfEmpty(String json) {
    if (json == null || EMPTY_JSON.matcher(json).matches()) {
      return null;
    }
    return json;
  }

  private static String serialize(Object object) throws Exception {
    return mapper.writerWithDefaultPrettyPrinter().writeValueAsString(object);
  }

  public static String serializeNullIfErrorOrEmpty(Object object) {
    try {
      return nullIfEmpty(serialize(object));
    } catch (Exception ex) {
      log.atSevere().withCause(ex).log("Failed to serialize json");
      return null;
    }
  }

  public static String serializeThrowIfError(Object object) {
    try {
      return serialize(object);
    } catch (Exception ex) {
      log.atSevere().withCause(ex).log("Failed to serialize json");
      throw new RuntimeException(ex);
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
