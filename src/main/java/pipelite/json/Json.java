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
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import java.util.regex.Pattern;
import lombok.extern.flogger.Flogger;
import pipelite.executor.TaskExecutor;

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
