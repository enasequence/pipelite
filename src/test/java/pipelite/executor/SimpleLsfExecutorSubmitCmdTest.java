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
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import pipelite.UniqueStringGenerator;
import pipelite.stage.Stage;
import pipelite.stage.parameters.SimpleLsfExecutorParameters;

public class SimpleLsfExecutorSubmitCmdTest {

  private final String PIPELINE_NAME = "PIPELINE_NAME";
  private final String PROCESS_ID = "PROCESS_ID";
  private final String STAGE_NAME = "STAGE_NAME";

  @Test
  public void test() throws IOException {

    SimpleLsfExecutor executor = new SimpleLsfExecutor();
    executor.setCmd("test");
    executor.setExecutorParams(
        SimpleLsfExecutorParameters.builder()
            .workDir(Files.createTempDirectory("TEMP").toString())
            .cpu(2)
            .memory(1)
            .memoryTimeout(Duration.ofMinutes(1))
            .queue("TEST")
            .timeout(Duration.ofMinutes(1))
            .build());

    Stage stage = Stage.builder().stageName(STAGE_NAME).executor(executor).build();

    String outFile = executor.setOutFile(PIPELINE_NAME, PROCESS_ID, stage.getStageName());
    String cmd = executor.getCmd(PIPELINE_NAME, PROCESS_ID, stage);

    assertThat(cmd)
        .isEqualTo(
            "bsub -oo "
                + outFile
                + " -n 2 -M 1M -R \"rusage[mem=1M:duration=1]\" -W 1 -q TEST test");
  }
}
