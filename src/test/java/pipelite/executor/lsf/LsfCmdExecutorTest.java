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
package pipelite.executor.lsf;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import pipelite.TestConfiguration;
import pipelite.UniqueStringGenerator;
import pipelite.configuration.LsfTestConfiguration;
import pipelite.executor.SuccessStageExecutor;
import pipelite.executor.cmd.LsfCmdExecutor;
import pipelite.executor.cmd.runner.CmdRunner;
import pipelite.executor.cmd.runner.CmdRunnerResult;
import pipelite.executor.cmd.runner.LocalCmdRunner;
import pipelite.stage.Stage;
import pipelite.stage.StageExecutionResult;
import pipelite.stage.StageParameters;

@SpringBootTest(classes = TestConfiguration.class)
@ActiveProfiles("test")
public class LsfCmdExecutorTest {

  @Autowired private LsfTestConfiguration lsfTestConfiguration;

  private StageParameters stageParameters() {
    try {
      StageParameters stageParameters =
          StageParameters.builder()
              .workDir(Files.createTempDirectory("TEMP").toString())
              .cores(1)
              .memory(1)
              .memoryTimeout(Duration.ofMinutes(1))
              .queue("defaultQueue")
              .build();
      return stageParameters;
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  private Stage stage(StageParameters stageParameters) {
    return Stage.builder()
        .pipelineName(UniqueStringGenerator.randomPipelineName())
        .processId(UniqueStringGenerator.randomProcessId())
        .stageName(UniqueStringGenerator.randomStageName())
        .executor(new SuccessStageExecutor())
        .stageParameters(stageParameters)
        .build();
  }

  private static String getCommandline(StageExecutionResult result) {
    return result.getAttribute(StageExecutionResult.COMMAND);
  }

  private LsfCmdExecutor executor =
      new LsfCmdExecutor() {
        @Override
        public CmdRunner getCmdRunner() {
          return (cmd, stageParameters) -> new CmdRunnerResult(0, "Job <13454> is submitted", "");
        }

        @Override
        public String getCmd(Stage stage) {
          return "echo test";
        }
      };

  @Test
  public void testLocalCmdRunnerWriteFileToStdout() throws IOException {
    Stage stage = Stage.builder().build();
    stage.getStageParameters().setHost(lsfTestConfiguration.getHost());
    File file = File.createTempFile("pipelite-test", "");
    file.createNewFile();
    Files.write(file.toPath(), "test".getBytes());
    CmdRunnerResult runnerResult =
        LsfCmdExecutor.writeFileToStdout(new LocalCmdRunner(), file.getAbsolutePath(), stage);
    assertThat(runnerResult.getStdout()).isEqualTo("test");
  }

  @Test
  public void testLocalCmdRunnerWriteFileToStderr() throws IOException {
    Stage stage = Stage.builder().build();
    stage.getStageParameters().setHost(lsfTestConfiguration.getHost());
    File file = File.createTempFile("pipelite-test", "");
    file.createNewFile();
    Files.write(file.toPath(), "test".getBytes());
    CmdRunnerResult runnerResult =
        LsfCmdExecutor.writeFileToStderr(new LocalCmdRunner(), file.getAbsolutePath(), stage);
    assertThat(runnerResult.getStderr()).isEqualTo("test");
  }

  @Test
  public void testCmdArguments() {
    StageParameters stageParameters = stageParameters();

    String cmd = getCommandline(executor.execute(stage(stageParameters)));
    assertTrue(cmd.contains(" -M 1 -R rusage[mem=1:duration=1]"));
    assertTrue(cmd.contains(" -n 1"));
    assertTrue(cmd.contains(" -q defaultQueue"));
    assertTrue(cmd.contains(" -oo " + stageParameters.getWorkDir()));
    assertTrue(cmd.contains(" -eo " + stageParameters.getWorkDir()));
  }

  @Test
  public void testNoQueueCmdArgument() {
    StageParameters stageParameters = stageParameters();
    stageParameters.setQueue(null);

    String cmd = getCommandline(executor.execute(stage(stageParameters)));
    assertFalse(cmd.contains("-q "));
  }

  @Test
  public void testQueueCmdArgument() {
    StageParameters stageParameters = stageParameters();
    stageParameters.setQueue("queue");

    String cmd = getCommandline(executor.execute(stage(stageParameters)));
    assertTrue(cmd.contains("-q queue"));
  }

  @Test
  public void testMemoryAndCoresCmdArgument() {
    StageParameters stageParameters = stageParameters();
    stageParameters.setMemory(2000);
    stageParameters.setCores(12);

    String cmd = getCommandline(executor.execute(stage(stageParameters)));
    assertTrue(cmd.contains(" -M 2000 -R rusage[mem=2000:duration=1]"));
    assertTrue(cmd.contains(" -n 12"));
    assertTrue(cmd.contains(" -q defaultQueue"));
    assertTrue(cmd.contains(" -oo " + stageParameters.getWorkDir()));
    assertTrue(cmd.contains(" -eo " + stageParameters.getWorkDir()));
  }

  @Test
  public void testExtractJobIdSubmitted() {
    assertThat(
            LsfCmdExecutor.extractJobIdSubmitted(
                "Job <2848143> is submitted to default queue <research-rh74>."))
        .isEqualTo("2848143");

    assertThat(LsfCmdExecutor.extractJobIdSubmitted("Job <2848143> is submitted "))
        .isEqualTo("2848143");
  }

  @Test
  public void testExtractJobIdNotFound() {
    assertThat(LsfCmdExecutor.extractJobIdNotFound("Job <345654> is not found.")).isTrue();
    assertThat(LsfCmdExecutor.extractJobIdNotFound("Job <345654> is not found")).isTrue();
    assertThat(LsfCmdExecutor.extractJobIdNotFound("Job <345654> is ")).isFalse();
  }

  @Test
  public void testExtractExitCode() {
    assertThat(LsfCmdExecutor.extractExitCode("Exited with exit code 1")).isEqualTo("1");
    assertThat(LsfCmdExecutor.extractExitCode("Exited with exit code 3.")).isEqualTo("3");
  }
}