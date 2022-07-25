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
package pipelite.json;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.HashMap;
import lombok.Data;
import org.junit.jupiter.api.Test;

public class JsonTest {

  private static class A {
    public final HashMap<String, String> map = new HashMap<>();
  }

  @Test
  public void serializeEmpty() {
    Object object = new Object();
    assertThat(Json.serialize(object)).isEqualTo("{ }");
    assertThat(Json.serializeSafely(object)).isNull();

    HashMap<String, String> map = new HashMap<>();
    assertThat(Json.serialize(map)).isEqualTo("{ }");
    assertThat(Json.serializeSafely(map)).isNull();
    map.put("test", null);
    assertThat(Json.serialize(map)).isEqualTo("{ }");
    assertThat(Json.serializeSafely(map)).isNull();

    A a = new A();
    assertThat(Json.serialize(a)).isEqualTo("{ }");
    assertThat(Json.serializeSafely(a)).isNull();
    a.map.put("test", null);
    assertThat(Json.serialize(a)).isEqualTo("{ }");
    assertThat(Json.serializeSafely(a)).isNull();
  }

  @Test
  public void serializeNotEmpty() {
    HashMap<String, String> map = new HashMap<>();
    map.put("test", "test");
    assertThat(Json.serialize(map)).isEqualTo("{\n" + "  \"test\" : \"test\"\n" + "}");
    assertThat(Json.serializeSafely(map)).isEqualTo("{\n" + "  \"test\" : \"test\"\n" + "}");

    A a = new A();
    a.map.put("test", "test");
    assertThat(Json.serialize(a))
        .isEqualTo("{\n" + "  \"map\" : {\n" + "    \"test\" : \"test\"\n" + "  }\n" + "}");
    assertThat(Json.serializeSafely(a))
        .isEqualTo("{\n" + "  \"map\" : {\n" + "    \"test\" : \"test\"\n" + "  }\n" + "}");
  }

  @Test
  public void deserializeEmpty() {
    A a = Json.deserialize("{ }", A.class);
    assertThat(a.map.size()).isZero();
    a = Json.deserializeSafely("{ }", A.class);
    assertThat(a.map.size()).isZero();
  }

  @Test
  public void deserializeNotEmpty() {
    A a =
        Json.deserialize(
            "{\n" + "  \"map\" : {\n" + "    \"test\" : \"test\"\n" + "  }\n" + "}", A.class);
    assertThat(a.map.size()).isOne();
    assertThat(a.map.get("test")).isEqualTo("test");
    a =
        Json.deserializeSafely(
            "{\n" + "  \"map\" : {\n" + "    \"test\" : \"test\"\n" + "  }\n" + "}", A.class);
    assertThat(a.map.size()).isOne();
    assertThat(a.map.get("test")).isEqualTo("test");
  }

  @Test
  public void deserializeInvalid() {
    boolean isError = false;
    try {
      A a =
          Json.deserialize(
              "{\n" + "  \"invalid\" : {\n" + "    \"test\" : \"test\"\n" + "  }\n" + "}", A.class);
    } catch (Exception ex) {
      isError = true;
    }
    assertThat(isError).isTrue();
    A a =
        Json.deserializeSafely(
            "{\n" + "  \"invalid\" : {\n" + "    \"test\" : \"test\"\n" + "  }\n" + "}", A.class);
    assertThat(a).isNull();
  }

  @Data
  private static class DurationSerializationTest {
    public Duration duration;
  }

  @Test
  public void durationSerializationTest() {
    Duration expected = Duration.ofMinutes(1);
    DurationSerializationTest test = new DurationSerializationTest();
    test.setDuration(expected);

    String json = Json.serialize(test);
    Duration actual = Json.deserialize(json, DurationSerializationTest.class).duration;

    assertThat(actual).isEqualTo(expected);
  }
}
