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
package pipelite.executor.cmd;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import pipelite.PipeliteTestConfiguration;
import pipelite.json.Json;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(classes = PipeliteTestConfiguration.class)
@ActiveProfiles("test")
public class LsfLocalCmdExecutorTest {

  @Test
  public void serialize() {
    String cmd = "echo test";
    String json = Json.serialize(new LsfLocalCmdExecutor(cmd));
    assertThat(json).isEqualTo("{\n" + "  \"cmd\" : \"echo test\"\n" + "}");
    assertThat(Json.deserialize(json, LsfLocalCmdExecutor.class).getCmd()).isEqualTo(cmd);
  }
}
