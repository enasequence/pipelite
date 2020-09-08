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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;
import org.junit.jupiter.api.Test;

public class JsonTest {

  @Test
  public void testEmpty() {
    Object object = new Object();
    assertThat(Json.serializeThrowIfError(object)).isEqualTo("{ }");
    assertThat(Json.serializeNullIfErrorOrEmpty(object)).isNull();

    HashMap<String, String> map = new HashMap<>();
    assertThat(Json.serializeThrowIfError(map)).isEqualTo("{ }");
    assertThat(Json.serializeNullIfErrorOrEmpty(map)).isNull();
    map.put("test", null);
    assertThat(Json.serializeThrowIfError(map)).isEqualTo("{ }");
    assertThat(Json.serializeNullIfErrorOrEmpty(map)).isNull();
  }

  @Test
  public void testNotEmpty() {
    HashMap<String, String> map = new HashMap<>();
    map.put("test", "test");
    assertThat(Json.serializeThrowIfError(map)).isEqualTo("{\n" + "  \"test\" : \"test\"\n" + "}");
    assertThat(Json.serializeNullIfErrorOrEmpty(map))
        .isEqualTo("{\n" + "  \"test\" : \"test\"\n" + "}");
  }
}
