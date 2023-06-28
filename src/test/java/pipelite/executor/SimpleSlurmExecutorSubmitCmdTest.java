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
package pipelite.executor;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import pipelite.stage.Stage;
import pipelite.stage.executor.StageExecutorRequest;
import pipelite.stage.parameters.SimpleSlurmExecutorParameters;
import pipelite.stage.path.SlurmLogFilePathResolver;

public class SimpleSlurmExecutorSubmitCmdTest {

  private static final String PIPELINE_NAME = "PIPELINE_NAME";
  private static final String PROCESS_ID = "PROCESS_ID";
  private static final String STAGE_NAME = "STAGE_NAME";

  @Test
  public void cmdMemUnitsM() throws IOException {

    SimpleSlurmExecutor executor = new SimpleSlurmExecutor();
    executor.setCmd("test");
    SimpleSlurmExecutorParameters params =
        SimpleSlurmExecutorParameters.builder()
            .logDir(Files.createTempDirectory("TEMP").toString())
            .cpu(2)
            .memory(1)
            .memoryUnits("M")
            .queue("TEST")
            .timeout(Duration.ofMinutes(1))
            .build();
    executor.setExecutorParams(params);

    Stage stage = Stage.builder().stageName(STAGE_NAME).executor(executor).build();
    StageExecutorRequest request =
        StageExecutorRequest.builder()
            .pipelineName(PIPELINE_NAME)
            .processId(PROCESS_ID)
            .stage(stage)
            .build();

    String logDir = new SlurmLogFilePathResolver().resolvedPath().dir(request);
    String logFileName = new SlurmLogFilePathResolver().fileName(request);

    executor.setOutFile(new SlurmLogFilePathResolver().resolvedPath().dir(request));

    String submitCmd = executor.getSubmitCmd(request);
    assertThat(submitCmd)
        .isEqualTo(
            "sbatch"
                + " --job-name=\""
                + PIPELINE_NAME
                + ":"
                + STAGE_NAME
                + ":"
                + PROCESS_ID
                + "\""
                + " --output=\""
                + logDir
                + "/"
                + logFileName
                + "\""
                + " -n 2"
                + " --mem=\"1M\""
                + " -t 1"
                + " -p TEST"
                + " --wrap=\"test\"");
  }

  @Test
  public void cmdNoMemUnits() throws IOException {

    SimpleSlurmExecutor executor = new SimpleSlurmExecutor();
    executor.setCmd("test");
    SimpleSlurmExecutorParameters params =
        SimpleSlurmExecutorParameters.builder()
            .logDir(Files.createTempDirectory("TEMP").toString())
            .cpu(2)
            .memory(1)
            .queue("TEST")
            .timeout(Duration.ofMinutes(1))
            .build();
    executor.setExecutorParams(params);

    Stage stage = Stage.builder().stageName(STAGE_NAME).executor(executor).build();
    StageExecutorRequest request =
        StageExecutorRequest.builder()
            .pipelineName(PIPELINE_NAME)
            .processId(PROCESS_ID)
            .stage(stage)
            .build();

    String logDir = new SlurmLogFilePathResolver().resolvedPath().dir(request);
    String logFileName = new SlurmLogFilePathResolver().fileName(request);

    executor.setOutFile(new SlurmLogFilePathResolver().resolvedPath().dir(request));

    String submitCmd = executor.getSubmitCmd(request);
    assertThat(submitCmd)
        .isEqualTo(
            "sbatch"
                + " --job-name=\""
                + PIPELINE_NAME
                + ":"
                + STAGE_NAME
                + ":"
                + PROCESS_ID
                + "\""
                + " --output=\""
                + logDir
                + "/"
                + logFileName
                + "\""
                + " -n 2"
                + " --mem=\"1\""
                + " -t 1"
                + " -p TEST"
                + " --wrap=\"test\"");
  }

  @Test
  public void cmdAccount() throws IOException {

    SimpleSlurmExecutor executor = new SimpleSlurmExecutor();
    executor.setCmd("test");
    SimpleSlurmExecutorParameters params =
        SimpleSlurmExecutorParameters.builder()
            .logDir(Files.createTempDirectory("TEMP").toString())
            .cpu(2)
            .memory(1)
            .memoryUnits("M")
            .account("ACCOUNT")
            .queue("TEST")
            .timeout(Duration.ofMinutes(1))
            .build();
    executor.setExecutorParams(params);

    Stage stage = Stage.builder().stageName(STAGE_NAME).executor(executor).build();
    StageExecutorRequest request =
        StageExecutorRequest.builder()
            .pipelineName(PIPELINE_NAME)
            .processId(PROCESS_ID)
            .stage(stage)
            .build();

    String logDir = new SlurmLogFilePathResolver().resolvedPath().dir(request);
    String logFileName = new SlurmLogFilePathResolver().fileName(request);

    executor.setOutFile(new SlurmLogFilePathResolver().resolvedPath().dir(request));

    String submitCmd = executor.getSubmitCmd(request);
    assertThat(submitCmd)
        .isEqualTo(
            "sbatch"
                + " --job-name=\""
                + PIPELINE_NAME
                + ":"
                + STAGE_NAME
                + ":"
                + PROCESS_ID
                + "\""
                + " --output=\""
                + logDir
                + "/"
                + logFileName
                + "\""
                + " -n 2"
                + " --mem=\"1M\""
                + " -t 1"
                + " -A ACCOUNT"
                + " -p TEST"
                + " --wrap=\"test\"");
  }
}
