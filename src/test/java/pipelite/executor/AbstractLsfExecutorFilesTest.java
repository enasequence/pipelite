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

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import org.junit.jupiter.api.Test;
import pipelite.exception.PipeliteException;
import pipelite.executor.cmd.CmdRunnerResult;
import pipelite.executor.cmd.LocalCmdRunner;
import pipelite.stage.Stage;
import pipelite.stage.executor.StageExecutorRequest;
import pipelite.stage.parameters.CmdExecutorParameters;

public class AbstractLsfExecutorFilesTest {

  private static class TestLsfExecutor extends AbstractLsfExecutor<CmdExecutorParameters> {
    @Override
    protected String getSubmitCmd(StageExecutorRequest request) {
      throw new PipeliteException("");
    }
  }

  @Test
  public void getWorkDir() {
    TestLsfExecutor executor = new TestLsfExecutor();
    Stage stage = Stage.builder().stageName("STAGE_NAME").executor(executor).build();
    StageExecutorRequest request =
        StageExecutorRequest.builder()
            .pipelineName("PIPELINE_NAME")
            .processId("PROCESS_ID")
            .stage(stage)
            .build();
    CmdExecutorParameters params = CmdExecutorParameters.builder().workDir("WORKDIR").build();

    assertThat(AbstractLsfExecutor.getWorkDir(request, params))
        .isEqualTo(Paths.get("WORKDIR/pipelite/PIPELINE_NAME/PROCESS_ID"));

    assertThat(AbstractLsfExecutor.getWorkDir(request, null))
        .isEqualTo(Paths.get("pipelite/PIPELINE_NAME/PROCESS_ID"));
  }

  @Test
  public void outFile() {
    TestLsfExecutor executor = new TestLsfExecutor();
    Stage stage = Stage.builder().stageName("STAGE_NAME").executor(executor).build();
    StageExecutorRequest request =
        StageExecutorRequest.builder()
            .pipelineName("PIPELINE_NAME")
            .processId("PROCESS_ID")
            .stage(stage)
            .build();
    CmdExecutorParameters params = CmdExecutorParameters.builder().workDir("WORKDIR").build();

    assertThat(AbstractLsfExecutor.getOutFile(request, params))
        .isEqualTo("WORKDIR/pipelite/PIPELINE_NAME/PROCESS_ID/STAGE_NAME.out");
  }

  @Test
  public void writeFileToStdout() throws IOException {
    File file = File.createTempFile("pipelite-test", "");
    file.createNewFile();
    Files.write(file.toPath(), "test".getBytes());
    CmdRunnerResult runnerResult =
        AbstractLsfExecutor.writeFileToStdout(
            new LocalCmdRunner(), file.getAbsolutePath(), CmdExecutorParameters.builder().build());
    assertThat(runnerResult.getStdout()).isEqualTo("test");
  }

  @Test
  public void writeFileToStderr() throws IOException {
    File file = File.createTempFile("pipelite-test", "");
    file.createNewFile();
    Files.write(file.toPath(), "test".getBytes());
    CmdRunnerResult runnerResult =
        AbstractLsfExecutor.writeFileToStderr(
            new LocalCmdRunner(), file.getAbsolutePath(), CmdExecutorParameters.builder().build());
    assertThat(runnerResult.getStderr()).isEqualTo("test");
  }
}
