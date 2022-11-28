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

import org.junit.jupiter.api.Test;
import pipelite.stage.Stage;
import pipelite.stage.executor.StageExecutorRequest;
import pipelite.stage.parameters.SimpleSlurmExecutorParameters;
import pipelite.stage.path.SlurmLogFilePathResolver;

import java.io.IOException;
import java.nio.file.Files;
import java.time.Duration;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

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
                "sbatch << EOF\n" +
                        "#!/bin/bash\n" +
                        "#SBATCH --job-name=\"" + PIPELINE_NAME + ":" + STAGE_NAME + ":" + PROCESS_ID + "\"\n" +
                        "#SBATCH --output=\"/dev/null\"\n" +
                        "#SBATCH --error=\"/dev/null\"\n" +
                        "#SBATCH -n 2\n" +
                        "#SBATCH --mem=\"1M\"\n" +
                        "#SBATCH -t 1\n" +
                        "#SBATCH -p TEST\n" +
                        "mkdir -p " + logDir + "\n" +
                        "test > " + logDir + "/" + logFileName + " 2>&1\n" +
                        "EOF");
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
                    "sbatch << EOF\n" +
                            "#!/bin/bash\n" +
                            "#SBATCH --job-name=\"" + PIPELINE_NAME + ":" + STAGE_NAME + ":" + PROCESS_ID + "\"\n" +
                            "#SBATCH --output=\"/dev/null\"\n" +
                            "#SBATCH --error=\"/dev/null\"\n" +
                            "#SBATCH -n 2\n" +
                            "#SBATCH --mem=\"1\"\n" +
                            "#SBATCH -t 1\n" +
                            "#SBATCH -p TEST\n" +
                            "mkdir -p " + logDir + "\n" +
                            "test > " + logDir + "/" + logFileName + " 2>&1\n" +
                            "EOF");
  }
}
