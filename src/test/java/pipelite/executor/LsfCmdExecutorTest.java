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

@SpringBootTest(classes = PipeliteTestConfiguration.class)
@ActiveProfiles(value = {"hsql-test", "pipelite-test"})
public class LsfCmdExecutorTest {

  @Autowired private LsfTestConfiguration lsfTestConfiguration;

  private final String PIPELINE_NAME = UniqueStringGenerator.randomPipelineName();
  private final String PROCESS_ID = UniqueStringGenerator.randomProcessId();

  private StageExecutorParameters executorParams() {
    try {
      StageExecutorParameters executorParams =
          StageExecutorParameters.builder()
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

  private Stage stage(StageExecutorParameters executorParams) {
    return Stage.builder()
        .stageName(UniqueStringGenerator.randomStageName())
        .executor(new EmptySyncStageExecutor(StageExecutorResultType.SUCCESS))
        .executorParams(executorParams)
        .build();
  }

  private static String getCommandline(StageExecutorResult result) {
    return result.getAttribute(StageExecutorResultAttribute.COMMAND);
  }

  private static LsfCmdExecutor createExecutor() {
    LsfCmdExecutor lsfCmdExecutor = new LsfCmdExecutor();
    lsfCmdExecutor.setCmd("echo test");
    lsfCmdExecutor.setCmdRunner(
        (cmd, executorParams) -> new CmdRunnerResult(0, "Job <13454> is submitted", "", null));
    return lsfCmdExecutor;
  }

  @Test
  public void testLocalCmdRunnerWriteFileToStdout() throws IOException {
    Stage stage =
        Stage.builder()
            .stageName(UniqueStringGenerator.randomStageName())
            .executor(new EmptySyncStageExecutor(StageExecutorResultType.SUCCESS))
            .build();
    stage.getExecutorParams().setHost(lsfTestConfiguration.getHost());
    File file = File.createTempFile("pipelite-test", "");
    file.createNewFile();
    Files.write(file.toPath(), "test".getBytes());
    CmdRunnerResult runnerResult =
        LsfCmdExecutor.writeFileToStdout(new LocalCmdRunner(), file.getAbsolutePath(), stage);
    assertThat(runnerResult.getStdout()).isEqualTo("test");
  }

  @Test
  public void testLocalCmdRunnerWriteFileToStderr() throws IOException {
    Stage stage =
        Stage.builder()
            .stageName(UniqueStringGenerator.randomStageName())
            .executor(new EmptySyncStageExecutor(StageExecutorResultType.SUCCESS))
            .build();
    stage.getExecutorParams().setHost(lsfTestConfiguration.getHost());
    File file = File.createTempFile("pipelite-test", "");
    file.createNewFile();
    Files.write(file.toPath(), "test".getBytes());
    CmdRunnerResult runnerResult =
        LsfCmdExecutor.writeFileToStderr(new LocalCmdRunner(), file.getAbsolutePath(), stage);
    assertThat(runnerResult.getStderr()).isEqualTo("test");
  }

  @Test
  public void testCmdArguments() {
    StageExecutorParameters executorParams = executorParams();
    LsfCmdExecutor executor = createExecutor();

    String cmd = getCommandline(executor.execute(PIPELINE_NAME, PROCESS_ID, stage(executorParams)));
    assertTrue(cmd.contains(" -M 1M -R \"rusage[mem=1M:duration=1]\""));
    assertTrue(cmd.contains(" -n 1"));
    assertTrue(cmd.contains(" -q defaultQueue"));
    assertTrue(cmd.contains(" -oo " + executorParams.getWorkDir()));
  }

  @Test
  public void testNoQueueCmdArgument() {
    StageExecutorParameters executorParams = executorParams();
    executorParams.setQueue(null);
    LsfCmdExecutor executor = createExecutor();

    String cmd = getCommandline(executor.execute(PIPELINE_NAME, PROCESS_ID, stage(executorParams)));
    assertFalse(cmd.contains("-q "));
  }

  @Test
  public void testQueueCmdArgument() {
    StageExecutorParameters executorParams = executorParams();
    executorParams.setQueue("queue");
    LsfCmdExecutor executor = createExecutor();

    String cmd = getCommandline(executor.execute(PIPELINE_NAME, PROCESS_ID, stage(executorParams)));
    assertTrue(cmd.contains("-q queue"));
  }

  @Test
  public void testMemoryAndCoresCmdArgument() {
    StageExecutorParameters executorParams = executorParams();
    executorParams.setMemory(2000);
    executorParams.setCores(12);
    LsfCmdExecutor executor = createExecutor();

    String cmd = getCommandline(executor.execute(PIPELINE_NAME, PROCESS_ID, stage(executorParams)));
    assertTrue(cmd.contains(" -M 2000M -R \"rusage[mem=2000M:duration=1]\""));
    assertTrue(cmd.contains(" -n 12"));
    assertTrue(cmd.contains(" -q defaultQueue"));
    assertTrue(cmd.contains(" -oo " + executorParams.getWorkDir()));
  }

  @Test
  public void testExtractJobIdSubmitted() {
    assertThat(
            LsfCmdExecutor.extractBsubJobIdSubmitted(
                "Job <2848143> is submitted to default queue <research-rh74>."))
        .isEqualTo("2848143");

    assertThat(LsfCmdExecutor.extractBsubJobIdSubmitted("Job <2848143> is submitted "))
        .isEqualTo("2848143");
  }

  @Test
  public void testExtractJobIdNotFound() {
    assertThat(LsfCmdExecutor.extractBjobsJobIdNotFound("Job <345654> is not found.")).isTrue();
    assertThat(LsfCmdExecutor.extractBjobsJobIdNotFound("Job <345654> is not found")).isTrue();
    assertThat(LsfCmdExecutor.extractBjobsJobIdNotFound("Job <345654> is ")).isFalse();
  }

  @Test
  public void testExtractExitCode() {
    assertThat(LsfCmdExecutor.extractBjobsExitCode("Exited with exit code 1")).isEqualTo("1");
    assertThat(LsfCmdExecutor.extractBjobsExitCode("Exited with exit code 3.")).isEqualTo("3");
  }

  @Test
  public void serializeNullCmdRunner() {
    String cmd = "echo test";
    LsfCmdExecutor lsfCmdExecutor = StageExecutor.createLsfCmdExecutor(cmd, null);
    lsfCmdExecutor.setJobId("test");
    lsfCmdExecutor.setStdoutFile("test");
    ZonedDateTime startTime =
        ZonedDateTime.of(LocalDateTime.of(2020, 1, 1, 1, 1), ZoneId.of("UTC"));
    lsfCmdExecutor.setStartTime(startTime);
    String json = Json.serialize(lsfCmdExecutor);
    assertThat(json)
        .isEqualTo(
            "{\n"
                + "  \"cmd\" : \"echo test\",\n"
                + "  \"jobId\" : \"test\",\n"
                + "  \"stdoutFile\" : \"test\",\n"
                + "  \"startTime\" : \"2020-01-01T01:01:00Z\"\n"
                + "}");
    LsfCmdExecutor deserializedLsfCmdExecutor = Json.deserialize(json, LsfCmdExecutor.class);
    assertThat(deserializedLsfCmdExecutor.getCmd()).isEqualTo(cmd);
    assertThat(deserializedLsfCmdExecutor.getCmdRunner()).isNull();
    assertThat(deserializedLsfCmdExecutor.getJobId()).isEqualTo("test");
    assertThat(deserializedLsfCmdExecutor.getStdoutFile()).isEqualTo("test");
    assertThat(deserializedLsfCmdExecutor.getStartTime()).isEqualTo(startTime);
  }

  @Test
  public void serializeLocalCmdRunner() {
    String cmd = "echo test";
    LsfCmdExecutor lsfCmdExecutor = StageExecutor.createLsfLocalCmdExecutor(cmd);
    lsfCmdExecutor.setJobId("test");
    lsfCmdExecutor.setStdoutFile("test");
    ZonedDateTime startTime =
        ZonedDateTime.of(LocalDateTime.of(2020, 1, 1, 1, 1), ZoneId.of("UTC"));
    lsfCmdExecutor.setStartTime(startTime);
    String json = Json.serialize(lsfCmdExecutor);
    assertThat(json)
        .isEqualTo(
            "{\n"
                + "  \"cmd\" : \"echo test\",\n"
                + "  \"cmdRunner\" : \"pipelite.executor.cmd.LocalCmdRunner\",\n"
                + "  \"jobId\" : \"test\",\n"
                + "  \"stdoutFile\" : \"test\",\n"
                + "  \"startTime\" : \"2020-01-01T01:01:00Z\"\n"
                + "}");
    LsfCmdExecutor deserializedLsfCmdExecutor = Json.deserialize(json, LsfCmdExecutor.class);
    assertThat(deserializedLsfCmdExecutor.getCmd()).isEqualTo(cmd);
    assertThat(deserializedLsfCmdExecutor.getCmdRunner()).isInstanceOf(LocalCmdRunner.class);
    assertThat(deserializedLsfCmdExecutor.getJobId()).isEqualTo("test");
    assertThat(deserializedLsfCmdExecutor.getStdoutFile()).isEqualTo("test");
    assertThat(deserializedLsfCmdExecutor.getStartTime()).isEqualTo(startTime);
  }

  @Test
  public void serializeSshCmdRunner() {
    String cmd = "echo test";
    LsfCmdExecutor lsfCmdExecutor = StageExecutor.createLsfSshCmdExecutor(cmd);
    lsfCmdExecutor.setJobId("test");
    lsfCmdExecutor.setStdoutFile("test");
    ZonedDateTime startTime =
        ZonedDateTime.of(LocalDateTime.of(2020, 1, 1, 1, 1), ZoneId.of("UTC"));
    lsfCmdExecutor.setStartTime(startTime);
    String json = Json.serialize(lsfCmdExecutor);
    assertThat(json)
        .isEqualTo(
            "{\n"
                + "  \"cmd\" : \"echo test\",\n"
                + "  \"cmdRunner\" : \"pipelite.executor.cmd.SshCmdRunner\",\n"
                + "  \"jobId\" : \"test\",\n"
                + "  \"stdoutFile\" : \"test\",\n"
                + "  \"startTime\" : \"2020-01-01T01:01:00Z\"\n"
                + "}");
    LsfCmdExecutor deserializedLsfCmdExecutor = Json.deserialize(json, LsfCmdExecutor.class);
    assertThat(deserializedLsfCmdExecutor.getCmd()).isEqualTo(cmd);
    assertThat(deserializedLsfCmdExecutor.getCmdRunner()).isInstanceOf(SshCmdRunner.class);
    assertThat(deserializedLsfCmdExecutor.getJobId()).isEqualTo("test");
    assertThat(deserializedLsfCmdExecutor.getStdoutFile()).isEqualTo("test");
    assertThat(deserializedLsfCmdExecutor.getStartTime()).isEqualTo(startTime);
  }
}
