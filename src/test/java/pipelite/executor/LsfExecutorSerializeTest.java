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

import org.junit.jupiter.api.Test;
import pipelite.executor.cmd.LocalCmdRunner;
import pipelite.executor.cmd.SshCmdRunner;
import pipelite.json.Json;
import pipelite.stage.Stage;
import pipelite.stage.executor.StageExecutor;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;

import static org.assertj.core.api.Assertions.assertThat;

public class LsfExecutorSerializeTest {

  @Test
  public void localLsfExecutor() {
    String cmd = "echo test";
    LsfExecutor executor = StageExecutor.createLocalLsfExecutor(cmd);
    executor.setJobId("test");
    executor.setOutFile("test");
    executor.setDefinitionFile(
        "PIPELINE_NAME",
        "PROCESS_ID",
        Stage.builder().stageName("STAGE_NAME").executor(executor).build());
    ZonedDateTime startTime =
        ZonedDateTime.of(LocalDateTime.of(2020, 1, 1, 1, 1), ZoneId.of("UTC"));
    executor.setStartTime(startTime);
    String json = Json.serialize(executor);
    assertThat(json)
        .isEqualTo(
            "{\n"
                + "  \"cmd\" : \"echo test\",\n"
                + "  \"cmdRunner\" : \"pipelite.executor.cmd.LocalCmdRunner\",\n"
                + "  \"jobId\" : \"test\",\n"
                + "  \"startTime\" : \"2020-01-01T01:01:00Z\",\n"
                + "  \"outFile\" : \"test\",\n"
                + "  \"definitionFile\" : \"pipelite/PIPELINE_NAME/PROCESS_ID/STAGE_NAME.job\"\n"
                + "}");
    LsfExecutor deserializedLsfExecutor = Json.deserialize(json, LsfExecutor.class);
    assertThat(deserializedLsfExecutor.getCmd()).isEqualTo(cmd);
    assertThat(deserializedLsfExecutor.getCmdRunner()).isInstanceOf(LocalCmdRunner.class);
    assertThat(deserializedLsfExecutor.getJobId()).isEqualTo("test");
    assertThat(deserializedLsfExecutor.getOutFile()).isEqualTo("test");
    assertThat(deserializedLsfExecutor.getStartTime()).isEqualTo(startTime);
  }

  @Test
  public void sshLsfExecutor() {
    String cmd = "echo test";
    LsfExecutor executor = StageExecutor.createSshLsfExecutor(cmd);
    executor.setJobId("test");
    executor.setOutFile("test");
    executor.setDefinitionFile(
        "PIPELINE_NAME",
        "PROCESS_ID",
        Stage.builder().stageName("STAGE_NAME").executor(executor).build());
    ZonedDateTime startTime =
        ZonedDateTime.of(LocalDateTime.of(2020, 1, 1, 1, 1), ZoneId.of("UTC"));
    executor.setStartTime(startTime);
    String json = Json.serialize(executor);
    assertThat(json)
        .isEqualTo(
            "{\n"
                + "  \"cmd\" : \"echo test\",\n"
                + "  \"cmdRunner\" : \"pipelite.executor.cmd.SshCmdRunner\",\n"
                + "  \"jobId\" : \"test\",\n"
                + "  \"startTime\" : \"2020-01-01T01:01:00Z\",\n"
                + "  \"outFile\" : \"test\",\n"
                + "  \"definitionFile\" : \"pipelite/PIPELINE_NAME/PROCESS_ID/STAGE_NAME.job\"\n"
                + "}");
    LsfExecutor deserializedLsfExecutor = Json.deserialize(json, LsfExecutor.class);
    assertThat(deserializedLsfExecutor.getCmd()).isEqualTo(cmd);
    assertThat(deserializedLsfExecutor.getCmdRunner()).isInstanceOf(SshCmdRunner.class);
    assertThat(deserializedLsfExecutor.getJobId()).isEqualTo("test");
    assertThat(deserializedLsfExecutor.getOutFile()).isEqualTo("test");
    assertThat(deserializedLsfExecutor.getStartTime()).isEqualTo(startTime);
  }
}
