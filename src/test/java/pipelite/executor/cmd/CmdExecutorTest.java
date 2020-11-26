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

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import pipelite.PipeliteTestConfiguration;
import pipelite.executor.StageExecutor;
import pipelite.json.Json;

@SpringBootTest(classes = PipeliteTestConfiguration.class)
@ActiveProfiles(value = {"hsql-test", "pipelite-test"})
public class CmdExecutorTest {

  @Test
  public void serializeNullCmdRunner() {
    CmdExecutor cmdExecutor = StageExecutor.createCmdExecutor("echo test", null);
    String cmd = "echo test";
    String json = Json.serialize(cmdExecutor);
    assertThat(json).isEqualTo("{\n" + "  \"cmd\" : \"echo test\"\n" + "}");
    assertThat(Json.deserialize(json, CmdExecutor.class).getCmd()).isEqualTo(cmd);
  }

  @Test
  public void serializeLocalCmdRunner() {
    CmdExecutor cmdExecutor = StageExecutor.createLocalCmdExecutor("echo test");
    String cmd = "echo test";
    String json = Json.serialize(cmdExecutor);
    assertThat(json)
        .isEqualTo(
            "{\n"
                + "  \"cmd\" : \"echo test\",\n"
                + "  \"cmdRunnerType\" : \"LOCAL_CMD_RUNNER\"\n"
                + "}");
    assertThat(Json.deserialize(json, CmdExecutor.class).getCmd()).isEqualTo(cmd);
  }

  @Test
  public void serializeSshCmdRunner() {
    CmdExecutor cmdExecutor = StageExecutor.createSshCmdExecutor("echo test");
    String cmd = "echo test";
    String json = Json.serialize(cmdExecutor);
    assertThat(json)
        .isEqualTo(
            "{\n"
                + "  \"cmd\" : \"echo test\",\n"
                + "  \"cmdRunnerType\" : \"SSH_CMD_RUNNER\"\n"
                + "}");
    assertThat(Json.deserialize(json, CmdExecutor.class).getCmd()).isEqualTo(cmd);
  }
}
