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
import org.junit.jupiter.api.Test;
import pipelite.stage.Stage;
import pipelite.stage.executor.StageExecutorRequest;
import pipelite.stage.parameters.LsfExecutorParameters;
import pipelite.stage.path.LsfLogFilePathResolver;

public class LsfExecutorSubmitCmdTest {

  private static final String PIPELINE_NAME = "PIPELINE_NAME";
  private static final String PROCESS_ID = "PROCESS_ID";
  private static final String STAGE_NAME = "STAGE_NAME";

  @Test
  public void test() throws IOException {
    LsfExecutor executor = new LsfExecutor();
    executor.setCmd("test");
    LsfExecutorParameters params =
        LsfExecutorParameters.builder()
            .logDir(Files.createTempDirectory("TEMP").toString())
            .definition("pipelite/executor/lsf.yaml")
            .format(LsfExecutorParameters.Format.YAML)
            .build();
    executor.setExecutorParams(params);
    Stage stage = Stage.builder().stageName(STAGE_NAME).executor(executor).build();
    StageExecutorRequest request =
        StageExecutorRequest.builder()
            .pipelineName(PIPELINE_NAME)
            .processId(PROCESS_ID)
            .stage(stage)
            .build();

    String definitionFile = "tempFile";
    String logDir = "\"" + new LsfLogFilePathResolver().placeholderPath().dir(request) + "\"";
    String logFileName = new LsfLogFilePathResolver().fileName(request);

    executor.setOutFile(new LsfLogFilePathResolver().resolvedPath().file(request));
    executor.setDefinitionFile(definitionFile);
    String submitCmd = executor.getSubmitCmd(request);

    assertThat(submitCmd)
        .isEqualTo(
            "bsub"
                + " -outdir "
                + logDir
                + " -cwd "
                + logDir
                + " -oo "
                + logFileName
                + " -yaml "
                + definitionFile
                + " test");
  }
}
