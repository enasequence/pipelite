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
import org.springframework.boot.test.context.SpringBootTest;
import pipelite.PipeliteTestConfiguration;
import pipelite.executor.cmd.LocalCmdRunner;
import pipelite.executor.cmd.SshCmdRunner;
import pipelite.json.Json;
import pipelite.stage.executor.StageExecutor;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;

import static org.assertj.core.api.Assertions.assertThat;

public class SimpleLsfExecutorSerializeTest {

  @Test
  public void localSimpleLsfExecutor() {
    String cmd = "echo test";
    SimpleLsfExecutor executor = StageExecutor.createLocalSimpleLsfExecutor(cmd);
    executor.setJobId("test");
    executor.setStdoutFile("test");
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
                + "  \"stdoutFile\" : \"test\"\n"
                + "}");
    SimpleLsfExecutor deserializedLsfExecutor = Json.deserialize(json, SimpleLsfExecutor.class);
    assertThat(deserializedLsfExecutor.getCmd()).isEqualTo(cmd);
    assertThat(deserializedLsfExecutor.getCmdRunner()).isInstanceOf(LocalCmdRunner.class);
    assertThat(deserializedLsfExecutor.getJobId()).isEqualTo("test");
    assertThat(deserializedLsfExecutor.getStdoutFile()).isEqualTo("test");
    assertThat(deserializedLsfExecutor.getStartTime()).isEqualTo(startTime);
  }

  @Test
  public void sshSimpleLsfExecutor() {
    String cmd = "echo test";
    SimpleLsfExecutor executor = StageExecutor.createSshSimpleLsfExecutor(cmd);
    executor.setJobId("test");
    executor.setStdoutFile("test");
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
                + "  \"stdoutFile\" : \"test\"\n"
                + "}");
    SimpleLsfExecutor deserializedLsfExecutor = Json.deserialize(json, SimpleLsfExecutor.class);
    assertThat(deserializedLsfExecutor.getCmd()).isEqualTo(cmd);
    assertThat(deserializedLsfExecutor.getCmdRunner()).isInstanceOf(SshCmdRunner.class);
    assertThat(deserializedLsfExecutor.getJobId()).isEqualTo("test");
    assertThat(deserializedLsfExecutor.getStdoutFile()).isEqualTo("test");
    assertThat(deserializedLsfExecutor.getStartTime()).isEqualTo(startTime);
  }
}
