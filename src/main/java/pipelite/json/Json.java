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
package pipelite.json;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.util.regex.Pattern;
import lombok.extern.flogger.Flogger;

@Flogger
public class Json {

  private Json() {}

  private static final Pattern EMPTY_JSON = Pattern.compile("^\\s*\\{\\s*\\}\\s*$");
  private static final ObjectMapper mapper = new ObjectMapper();

  static {
    mapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
    mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
    mapper.setSerializationInclusion(JsonInclude.Include.NON_EMPTY);

    mapper.registerModule(new JavaTimeModule());
    mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    mapper.disable(SerializationFeature.WRITE_DATE_TIMESTAMPS_AS_NANOSECONDS);
    mapper.disable(DeserializationFeature.READ_DATE_TIMESTAMPS_AS_NANOSECONDS);
  }

  /**
   * Serializes the object in JSON.
   *
   * @throws RuntimeException if the serialization fails.
   */
  public static String serialize(Object object) {
    try {
      return serializeThrowIfError(object);
    } catch (Exception ex) {
      log.atSevere().withCause(ex).log("Failed to serialize json");
      throw new RuntimeException(ex);
    }
  }

  /** Serializes the object in JSON. Returns null if the serialization fails or is empty. */
  public static String serializeSafely(Object object) {
    try {
      return serializeNullIfEmpty(serializeThrowIfError(object));
    } catch (Exception ex) {
      log.atSevere().withCause(ex).log("Failed to serialize json");
      return null;
    }
  }

  /**
   * Deserializes the object from JSON.
   *
   * @param json the JSON string.
   * @param className the name of the class to deserialize into.
   * @param clazz the return type.
   * @throws RuntimeException if the deserialization fails.
   */
  public static <T> T deserialize(String json, String className, Class<T> clazz) {
    try {
      return (T) mapper.readValue(json, Class.forName(className));
    } catch (Exception ex) {
      log.atSevere().withCause(ex).log("Failed to deserialize json");
      throw new RuntimeException(ex);
    }
  }

  /**
   * Deserializes the object from JSON.
   *
   * @param json the JSON string.
   * @param clazz the return type.
   * @throws RuntimeException if the deserialization fails.
   */
  public static <T> T deserialize(String json, Class<T> clazz) {
    try {
      return mapper.readValue(json, clazz);
    } catch (Exception ex) {
      log.atSevere().withCause(ex).log("Failed to deserialize json");
      throw new RuntimeException(ex);
    }
  }

  /**
   * Deserializes the object from JSON. Returns null if the deserialization fails.
   *
   * @param json the JSON string.
   * @param clazz the return type.
   */
  public static <T> T deserializeSafely(String json, Class<T> clazz) {
    try {
      return mapper.readValue(json, clazz);
    } catch (Exception ex) {
      log.atSevere().withCause(ex).log("Failed to deserialize json");
      return null;
    }
  }

  private static String serializeNullIfEmpty(String json) {
    if (json == null || EMPTY_JSON.matcher(json).matches()) {
      return null;
    }
    return json;
  }

  private static String serializeThrowIfError(Object object) throws Exception {
    return mapper.writerWithDefaultPrettyPrinter().writeValueAsString(object);
  }
}
