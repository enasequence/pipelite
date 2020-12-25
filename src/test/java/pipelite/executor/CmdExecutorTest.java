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
package pipelite.executor;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import pipelite.PipeliteTestConfiguration;
import pipelite.executor.cmd.LocalCmdRunner;
import pipelite.executor.cmd.SshCmdRunner;
import pipelite.json.Json;
import pipelite.stage.Stage;
import pipelite.stage.executor.EmptySyncStageExecutor;
import pipelite.stage.executor.StageExecutor;
import pipelite.stage.executor.StageExecutorParameters;
import pipelite.stage.executor.StageExecutorResultType;

@SpringBootTest(classes = PipeliteTestConfiguration.class)
@ActiveProfiles(value = {"hsql-test", "pipelite-test"})
public class CmdExecutorTest {

  @Test
  public void serializeNullCmdRunner() {
    CmdExecutor cmdExecutor = StageExecutor.createCmdExecutor("echo test", null);
    String cmd = "echo test";
    String json = Json.serialize(cmdExecutor);
    assertThat(json).isEqualTo("{\n" + "  \"cmd\" : \"echo test\"\n" + "}");
    CmdExecutor deserializedCmdExecutor = Json.deserialize(json, CmdExecutor.class);
    assertThat(deserializedCmdExecutor.getCmd()).isEqualTo(cmd);
    assertThat(deserializedCmdExecutor.getCmdRunner()).isNull();
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
                + "  \"cmdRunner\" : \"pipelite.executor.cmd.LocalCmdRunner\"\n"
                + "}");
    CmdExecutor deserializedCmdExecutor = Json.deserialize(json, CmdExecutor.class);
    assertThat(deserializedCmdExecutor.getCmd()).isEqualTo(cmd);
    assertThat(deserializedCmdExecutor.getCmdRunner()).isInstanceOf(LocalCmdRunner.class);
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
                + "  \"cmdRunner\" : \"pipelite.executor.cmd.SshCmdRunner\"\n"
                + "}");
    CmdExecutor deserializedCmdExecutor = Json.deserialize(json, CmdExecutor.class);
    assertThat(deserializedCmdExecutor.getCmd()).isEqualTo(cmd);
    assertThat(deserializedCmdExecutor.getCmdRunner()).isInstanceOf(SshCmdRunner.class);
  }

  private Stage createStage(String workDir) {
    return Stage.builder()
        .stageName("STAGE")
        .executor(new EmptySyncStageExecutor(StageExecutorResultType.SUCCESS))
        .executorParams(StageExecutorParameters.builder().workDir(workDir).build())
        .build();
  }

  @Test
  public void getWorkDir() {
    assertThat(CmdExecutor.getWorkDir("PIPELINE_NAME", "PROCESS_ID", createStage("WORKDIR ")))
        .isEqualTo("WORKDIR/pipelite/PIPELINE_NAME/PROCESS_ID");
    assertThat(CmdExecutor.getWorkDir("PIPELINE_NAME", "PROCESS_ID", createStage(null)))
        .isEqualTo("pipelite/PIPELINE_NAME/PROCESS_ID");
    assertThat(
            CmdExecutor.getWorkDir("PIPELINE_NAME", "PROCESS_ID", createStage("WORKDIR/DIR\\DIR/")))
        .isEqualTo("WORKDIR/DIR/DIR/pipelite/PIPELINE_NAME/PROCESS_ID");
  }

  @Test
  public void getOutFile() {
    assertThat(
            CmdExecutor.getOutFile("PIPELINE_NAME", "PROCESS_ID", createStage("WORKDIR "), "out"))
        .isEqualTo("WORKDIR/pipelite/PIPELINE_NAME/PROCESS_ID/PIPELINE_NAME_PROCESS_ID_STAGE.out");
  }
}
