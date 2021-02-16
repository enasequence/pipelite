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

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import pipelite.stage.Stage;
import pipelite.stage.executor.StageExecutorRequest;
import pipelite.stage.parameters.SimpleLsfExecutorParameters;

public class SimpleLsfExecutorSubmitCmdTest {

  private static final String PIPELINE_NAME = "PIPELINE_NAME";
  private static final String PROCESS_ID = "PROCESS_ID";
  private static final String STAGE_NAME = "STAGE_NAME";

  @Test
  public void cmdMemUnitsM() throws IOException {

    SimpleLsfExecutor executor = new SimpleLsfExecutor();
    executor.setCmd("test");
    SimpleLsfExecutorParameters params =
        SimpleLsfExecutorParameters.builder()
            .workDir(Files.createTempDirectory("TEMP").toString())
            .cpu(2)
            .memory(1)
            .memoryUnits("M")
            .memoryTimeout(Duration.ofMinutes(1))
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
    String outFile = AbstractLsfExecutor.getOutFile(request, params);
    executor.setOutFile(outFile);
    String submitCmd = executor.getSubmitCmd(request);
    assertThat(submitCmd)
        .isEqualTo(
            "bsub -oo "
                + outFile
                + " -n 2 -M 1M -R \"rusage[mem=1M:duration=1]\" -W 1 -q TEST test");
  }

  @Test
  public void cmdMemUnitsMNoDuration() throws IOException {

    SimpleLsfExecutor executor = new SimpleLsfExecutor();
    executor.setCmd("test");
    SimpleLsfExecutorParameters params =
        SimpleLsfExecutorParameters.builder()
            .workDir(Files.createTempDirectory("TEMP").toString())
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
    String outFile = AbstractLsfExecutor.getOutFile(request, params);
    executor.setOutFile(outFile);
    String submitCmd = executor.getSubmitCmd(request);
    assertThat(submitCmd)
        .isEqualTo("bsub -oo " + outFile + " -n 2 -M 1M -R \"rusage[mem=1M]\" -W 1 -q TEST test");
  }

  @Test
  public void cmdNoMemUnits() throws IOException {

    SimpleLsfExecutor executor = new SimpleLsfExecutor();
    executor.setCmd("test");
    SimpleLsfExecutorParameters params =
        SimpleLsfExecutorParameters.builder()
            .workDir(Files.createTempDirectory("TEMP").toString())
            .cpu(2)
            .memory(1)
            .memoryTimeout(Duration.ofMinutes(1))
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
    String outFile = AbstractLsfExecutor.getOutFile(request, params);
    executor.setOutFile(outFile);
    String submitCmd = executor.getSubmitCmd(request);
    assertThat(submitCmd)
        .isEqualTo(
            "bsub -oo " + outFile + " -n 2 -M 1 -R \"rusage[mem=1:duration=1]\" -W 1 -q TEST test");
  }

  @Test
  public void cmdNoMemUnitsNoDuration() throws IOException {

    SimpleLsfExecutor executor = new SimpleLsfExecutor();
    executor.setCmd("test");
    SimpleLsfExecutorParameters params =
        SimpleLsfExecutorParameters.builder()
            .workDir(Files.createTempDirectory("TEMP").toString())
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
    String outFile = AbstractLsfExecutor.getOutFile(request, params);
    executor.setOutFile(outFile);
    String submitCmd = executor.getSubmitCmd(request);
    assertThat(submitCmd)
        .isEqualTo("bsub -oo " + outFile + " -n 2 -M 1 -R \"rusage[mem=1]\" -W 1 -q TEST test");
  }
}
