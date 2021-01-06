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
import pipelite.stage.Stage;
import pipelite.stage.parameters.LsfExecutorParameters;

import java.io.IOException;
import java.nio.file.Files;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public class LsfExecutorSubmitCmdTest {

  private final String PIPELINE_NAME = "PIPELINE_NAME";
  private final String PROCESS_ID = "PROCESS_ID";
  private final String STAGE_NAME = "STAGE_NAME";

  @Test
  public void test() throws IOException {
    LsfExecutor executor = new LsfExecutor();
    executor.setCmd("test");
    executor.setExecutorParams(
        LsfExecutorParameters.builder()
            .workDir(Files.createTempDirectory("TEMP").toString())
            .definition("pipelite/executor/lsf.yaml")
            .format(LsfExecutorParameters.Format.YAML)
            .build());
    Stage stage = Stage.builder().stageName(STAGE_NAME).executor(executor).build();

    String outFile = executor.setOutFile(PIPELINE_NAME, PROCESS_ID, stage.getStageName());
    String definitionFile = executor.setDefinitionFile(PIPELINE_NAME, PROCESS_ID, stage);

    String cmd = executor.getCmd(PIPELINE_NAME, PROCESS_ID, stage);
    assertThat(cmd).isEqualTo("bsub -oo " + outFile + " -yaml " + definitionFile + " test");
  }
}
