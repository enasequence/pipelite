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
import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import pipelite.PipeliteTestConfiguration;
import pipelite.UniqueStringGenerator;
import pipelite.configuration.LsfTestConfiguration;
import pipelite.executor.cmd.CmdRunnerResult;
import pipelite.executor.cmd.LocalCmdRunner;
import pipelite.executor.cmd.SshCmdRunner;
import pipelite.json.Json;
import pipelite.stage.Stage;
import pipelite.stage.executor.*;
import pipelite.stage.parameters.LsfExecutorParameters;

@SpringBootTest(classes = PipeliteTestConfiguration.class)
@ActiveProfiles(value = {"hsql-test", "pipelite-test"})
public class LsfExecutorTest {

  @Autowired private LsfTestConfiguration lsfTestConfiguration;

  private final String PIPELINE_NAME = UniqueStringGenerator.randomPipelineName();
  private final String PROCESS_ID = UniqueStringGenerator.randomProcessId();

  private LsfExecutorParameters executorParams() {
    try {
      LsfExecutorParameters executorParams =
          LsfExecutorParameters.builder()
              .workDir(Files.createTempDirectory("TEMP").toString())
              .cores(1)
              .memory(1)
              .memoryTimeout(Duration.ofMinutes(1))
              .queue("defaultQueue")
              .build();
      return executorParams;
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  private Stage createStage(LsfExecutor executor) {
    return Stage.builder()
        .stageName(UniqueStringGenerator.randomStageName())
        .executor(executor)
        .build();
  }

  private static String getCommandline(StageExecutorResult result) {
    return result.getAttribute(StageExecutorResultAttribute.COMMAND);
  }

  private static LsfExecutor createExecutor(LsfExecutorParameters params) {
    LsfExecutor lsfExecutor = new LsfExecutor();
    lsfExecutor.setCmd("echo test");
    lsfExecutor.setExecutorParams(params);
    lsfExecutor.setCmdRunner(
        (cmd, executorParams) -> new CmdRunnerResult(0, "Job <13454> is submitted", "", null));
    return lsfExecutor;
  }

  @Test
  public void writeFileToStdout() throws IOException {
    File file = File.createTempFile("pipelite-test", "");
    file.createNewFile();
    Files.write(file.toPath(), "test".getBytes());
    CmdRunnerResult runnerResult =
        LsfExecutor.writeFileToStdout(
            new LocalCmdRunner(), file.getAbsolutePath(), LsfExecutorParameters.builder().build());
    assertThat(runnerResult.getStdout()).isEqualTo("test");
  }

  @Test
  public void writeFileToStderr() throws IOException {
    File file = File.createTempFile("pipelite-test", "");
    file.createNewFile();
    Files.write(file.toPath(), "test".getBytes());
    CmdRunnerResult runnerResult =
        LsfExecutor.writeFileToStderr(
            new LocalCmdRunner(), file.getAbsolutePath(), LsfExecutorParameters.builder().build());
    assertThat(runnerResult.getStderr()).isEqualTo("test");
  }

  @Test
  public void testCmdArguments() {
    LsfExecutorParameters executorParams = executorParams();
    LsfExecutor executor = createExecutor(executorParams);
    String cmd = getCommandline(executor.execute(PIPELINE_NAME, PROCESS_ID, createStage(executor)));
    assertTrue(cmd.contains(" -M 1M -R \"rusage[mem=1M:duration=1]\""));
    assertTrue(cmd.contains(" -n 1"));
    assertTrue(cmd.contains(" -q defaultQueue"));
    assertTrue(cmd.contains(" -oo " + executorParams.getWorkDir()));
  }

  @Test
  public void testNoQueueCmdArgument() {
    LsfExecutorParameters executorParams = executorParams();
    executorParams.setQueue(null);
    LsfExecutor executor = createExecutor(executorParams);

    String cmd = getCommandline(executor.execute(PIPELINE_NAME, PROCESS_ID, createStage(executor)));
    assertFalse(cmd.contains("-q "));
  }

  @Test
  public void testQueueCmdArgument() {
    LsfExecutorParameters executorParams = executorParams();
    executorParams.setQueue("queue");
    LsfExecutor executor = createExecutor(executorParams);

    String cmd = getCommandline(executor.execute(PIPELINE_NAME, PROCESS_ID, createStage(executor)));
    assertTrue(cmd.contains("-q queue"));
  }

  @Test
  public void testMemoryAndCoresCmdArgument() {
    LsfExecutorParameters executorParams = executorParams();
    executorParams.setMemory(2000);
    executorParams.setCores(12);
    LsfExecutor executor = createExecutor(executorParams);

    String cmd = getCommandline(executor.execute(PIPELINE_NAME, PROCESS_ID, createStage(executor)));
    assertTrue(cmd.contains(" -M 2000M -R \"rusage[mem=2000M:duration=1]\""));
    assertTrue(cmd.contains(" -n 12"));
    assertTrue(cmd.contains(" -q defaultQueue"));
    assertTrue(cmd.contains(" -oo " + executorParams.getWorkDir()));
  }

  @Test
  public void testExtractJobIdSubmitted() {
    assertThat(
            LsfExecutor.extractBsubJobIdSubmitted(
                "Job <2848143> is submitted to default queue <research-rh74>."))
        .isEqualTo("2848143");

    assertThat(LsfExecutor.extractBsubJobIdSubmitted("Job <2848143> is submitted "))
        .isEqualTo("2848143");
  }

  @Test
  public void testExtractJobIdNotFound() {
    assertThat(LsfExecutor.extractBjobsJobIdNotFound("Job <345654> is not found.")).isTrue();
    assertThat(LsfExecutor.extractBjobsJobIdNotFound("Job <345654> is not found")).isTrue();
    assertThat(LsfExecutor.extractBjobsJobIdNotFound("Job <345654> is ")).isFalse();
  }

  @Test
  public void testExtractExitCode() {
    assertThat(LsfExecutor.extractBjobsExitCode("Exited with exit code 1")).isEqualTo("1");
    assertThat(LsfExecutor.extractBjobsExitCode("Exited with exit code 3.")).isEqualTo("3");
  }

  @Test
  public void serializeNullCmdRunner() {
    String cmd = "echo test";
    LsfExecutor lsfExecutor = StageExecutor.createLsfExecutor(cmd, null);
    lsfExecutor.setJobId("test");
    lsfExecutor.setStdoutFile("test");
    ZonedDateTime startTime =
        ZonedDateTime.of(LocalDateTime.of(2020, 1, 1, 1, 1), ZoneId.of("UTC"));
    lsfExecutor.setStartTime(startTime);
    String json = Json.serialize(lsfExecutor);
    assertThat(json)
        .isEqualTo(
            "{\n"
                + "  \"cmd\" : \"echo test\",\n"
                + "  \"jobId\" : \"test\",\n"
                + "  \"stdoutFile\" : \"test\",\n"
                + "  \"startTime\" : \"2020-01-01T01:01:00Z\"\n"
                + "}");
    LsfExecutor deserializedLsfExecutor = Json.deserialize(json, LsfExecutor.class);
    assertThat(deserializedLsfExecutor.getCmd()).isEqualTo(cmd);
    assertThat(deserializedLsfExecutor.getCmdRunner()).isNull();
    assertThat(deserializedLsfExecutor.getJobId()).isEqualTo("test");
    assertThat(deserializedLsfExecutor.getStdoutFile()).isEqualTo("test");
    assertThat(deserializedLsfExecutor.getStartTime()).isEqualTo(startTime);
  }

  @Test
  public void serializeLocalCmdRunner() {
    String cmd = "echo test";
    LsfExecutor lsfExecutor = StageExecutor.createLocalLsfExecutor(cmd);
    lsfExecutor.setJobId("test");
    lsfExecutor.setStdoutFile("test");
    ZonedDateTime startTime =
        ZonedDateTime.of(LocalDateTime.of(2020, 1, 1, 1, 1), ZoneId.of("UTC"));
    lsfExecutor.setStartTime(startTime);
    String json = Json.serialize(lsfExecutor);
    assertThat(json)
        .isEqualTo(
            "{\n"
                + "  \"cmd\" : \"echo test\",\n"
                + "  \"cmdRunner\" : \"pipelite.executor.cmd.LocalCmdRunner\",\n"
                + "  \"jobId\" : \"test\",\n"
                + "  \"stdoutFile\" : \"test\",\n"
                + "  \"startTime\" : \"2020-01-01T01:01:00Z\"\n"
                + "}");
    LsfExecutor deserializedLsfExecutor = Json.deserialize(json, LsfExecutor.class);
    assertThat(deserializedLsfExecutor.getCmd()).isEqualTo(cmd);
    assertThat(deserializedLsfExecutor.getCmdRunner()).isInstanceOf(LocalCmdRunner.class);
    assertThat(deserializedLsfExecutor.getJobId()).isEqualTo("test");
    assertThat(deserializedLsfExecutor.getStdoutFile()).isEqualTo("test");
    assertThat(deserializedLsfExecutor.getStartTime()).isEqualTo(startTime);
  }

  @Test
  public void serializeSshCmdRunner() {
    String cmd = "echo test";
    LsfExecutor lsfExecutor = StageExecutor.createSshLsfExecutor(cmd);
    lsfExecutor.setJobId("test");
    lsfExecutor.setStdoutFile("test");
    ZonedDateTime startTime =
        ZonedDateTime.of(LocalDateTime.of(2020, 1, 1, 1, 1), ZoneId.of("UTC"));
    lsfExecutor.setStartTime(startTime);
    String json = Json.serialize(lsfExecutor);
    assertThat(json)
        .isEqualTo(
            "{\n"
                + "  \"cmd\" : \"echo test\",\n"
                + "  \"cmdRunner\" : \"pipelite.executor.cmd.SshCmdRunner\",\n"
                + "  \"jobId\" : \"test\",\n"
                + "  \"stdoutFile\" : \"test\",\n"
                + "  \"startTime\" : \"2020-01-01T01:01:00Z\"\n"
                + "}");
    LsfExecutor deserializedLsfExecutor = Json.deserialize(json, LsfExecutor.class);
    assertThat(deserializedLsfExecutor.getCmd()).isEqualTo(cmd);
    assertThat(deserializedLsfExecutor.getCmdRunner()).isInstanceOf(SshCmdRunner.class);
    assertThat(deserializedLsfExecutor.getJobId()).isEqualTo("test");
    assertThat(deserializedLsfExecutor.getStdoutFile()).isEqualTo("test");
    assertThat(deserializedLsfExecutor.getStartTime()).isEqualTo(startTime);
  }

  @Test
  public void getWorkDir() {
    LsfExecutor lsfExecutor = StageExecutor.createLocalLsfExecutor("");

    lsfExecutor.setExecutorParams(LsfExecutorParameters.builder().workDir("WORKDIR").build());
    assertThat(lsfExecutor.getWorkDir("PIPELINE_NAME", "PROCESS_ID"))
        .isEqualTo("WORKDIR/pipelite/PIPELINE_NAME/PROCESS_ID");

    lsfExecutor.setExecutorParams(LsfExecutorParameters.builder().workDir(null).build());
    assertThat(lsfExecutor.getWorkDir("PIPELINE_NAME", "PROCESS_ID"))
        .isEqualTo("pipelite/PIPELINE_NAME/PROCESS_ID");

    lsfExecutor.setExecutorParams(
        LsfExecutorParameters.builder().workDir("WORKDIR/DIR\\DIR/").build());
    assertThat(lsfExecutor.getWorkDir("PIPELINE_NAME", "PROCESS_ID"))
        .isEqualTo("WORKDIR/DIR/DIR/pipelite/PIPELINE_NAME/PROCESS_ID");
  }

  @Test
  public void getOutFile() {
    LsfExecutor lsfExecutor = StageExecutor.createLocalLsfExecutor("");

    lsfExecutor.setExecutorParams(LsfExecutorParameters.builder().workDir("WORKDIR").build());
    assertThat(lsfExecutor.getOutFile("PIPELINE_NAME", "PROCESS_ID", "STAGE", "out"))
        .isEqualTo("WORKDIR/pipelite/PIPELINE_NAME/PROCESS_ID/PIPELINE_NAME_PROCESS_ID_STAGE.out");
  }
}
