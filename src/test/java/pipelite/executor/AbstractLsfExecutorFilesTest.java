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
import pipelite.exception.PipeliteException;
import pipelite.executor.cmd.CmdRunnerResult;
import pipelite.executor.cmd.LocalCmdRunner;
import pipelite.stage.Stage;
import pipelite.stage.parameters.CmdExecutorParameters;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import static org.assertj.core.api.Assertions.assertThat;

public class AbstractLsfExecutorFilesTest {

  private static class TestLsfExecutor extends AbstractLsfExecutor<CmdExecutorParameters> {
    @Override
    protected String getSubmitCmd(String pipelineName, String processId, Stage stage) {
      throw new PipeliteException("");
    }
  }

  @Test
  public void getWorkDir() {
    TestLsfExecutor executor = new TestLsfExecutor();

    executor.setExecutorParams(CmdExecutorParameters.builder().workDir("WORKDIR").build());
    assertThat(executor.getWorkDir("PIPELINE_NAME", "PROCESS_ID"))
        .isEqualTo(Paths.get("WORKDIR/pipelite/PIPELINE_NAME/PROCESS_ID"));

    executor.setExecutorParams(CmdExecutorParameters.builder().workDir(null).build());
    assertThat(executor.getWorkDir("PIPELINE_NAME", "PROCESS_ID"))
        .isEqualTo(Paths.get("pipelite/PIPELINE_NAME/PROCESS_ID"));

    executor.setExecutorParams(CmdExecutorParameters.builder().workDir("WORKDIR/DIR/DIR/").build());
    assertThat(executor.getWorkDir("PIPELINE_NAME", "PROCESS_ID"))
        .isEqualTo(Paths.get("WORKDIR/DIR/DIR/pipelite/PIPELINE_NAME/PROCESS_ID"));

    executor.setExecutorParams(CmdExecutorParameters.builder().workDir(null).build());
    assertThat(executor.getWorkDir("PIPELINE_NAME", "PROCESS_ID"))
        .isEqualTo(Paths.get("pipelite/PIPELINE_NAME/PROCESS_ID"));
  }

  @Test
  public void outFile() {
    TestLsfExecutor executor = new TestLsfExecutor();

    executor.setExecutorParams(CmdExecutorParameters.builder().workDir("WORKDIR").build());
    executor.setOutFile("PIPELINE_NAME", "PROCESS_ID", "STAGE");
    assertThat(executor.getOutFile())
        .isEqualTo("WORKDIR/pipelite/PIPELINE_NAME/PROCESS_ID/STAGE.out");
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
