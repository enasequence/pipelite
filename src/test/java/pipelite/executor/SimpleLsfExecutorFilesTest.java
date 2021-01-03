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
import pipelite.executor.cmd.CmdRunnerResult;
import pipelite.executor.cmd.LocalCmdRunner;
import pipelite.stage.parameters.SimpleLsfExecutorParameters;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

import static org.assertj.core.api.Assertions.assertThat;

public class SimpleLsfExecutorFilesTest {

  @Test
  public void getWorkDir() {
    SimpleLsfExecutor executor = new SimpleLsfExecutor();

    executor.setExecutorParams(SimpleLsfExecutorParameters.builder().workDir("WORKDIR").build());
    assertThat(executor.getWorkDir("PIPELINE_NAME", "PROCESS_ID"))
        .isEqualTo("WORKDIR/pipelite/PIPELINE_NAME/PROCESS_ID");

    executor.setExecutorParams(SimpleLsfExecutorParameters.builder().workDir(null).build());
    assertThat(executor.getWorkDir("PIPELINE_NAME", "PROCESS_ID"))
        .isEqualTo("pipelite/PIPELINE_NAME/PROCESS_ID");

    executor.setExecutorParams(
        SimpleLsfExecutorParameters.builder().workDir("WORKDIR/DIR\\DIR/").build());
    assertThat(executor.getWorkDir("PIPELINE_NAME", "PROCESS_ID"))
        .isEqualTo("WORKDIR/DIR/DIR/pipelite/PIPELINE_NAME/PROCESS_ID");
  }

  @Test
  public void getOutFile() {
    SimpleLsfExecutor executor = new SimpleLsfExecutor();

    executor.setExecutorParams(SimpleLsfExecutorParameters.builder().workDir("WORKDIR").build());
    assertThat(executor.getOutFile("PIPELINE_NAME", "PROCESS_ID", "STAGE", "out"))
        .isEqualTo("WORKDIR/pipelite/PIPELINE_NAME/PROCESS_ID/PIPELINE_NAME_PROCESS_ID_STAGE.out");
  }

  @Test
  public void writeFileToStdout() throws IOException {
    File file = File.createTempFile("pipelite-test", "");
    file.createNewFile();
    Files.write(file.toPath(), "test".getBytes());
    CmdRunnerResult runnerResult =
        SimpleLsfExecutor.writeFileToStdout(
            new LocalCmdRunner(),
            file.getAbsolutePath(),
            SimpleLsfExecutorParameters.builder().build());
    assertThat(runnerResult.getStdout()).isEqualTo("test");
  }

  @Test
  public void writeFileToStderr() throws IOException {
    File file = File.createTempFile("pipelite-test", "");
    file.createNewFile();
    Files.write(file.toPath(), "test".getBytes());
    CmdRunnerResult runnerResult =
        SimpleLsfExecutor.writeFileToStderr(
            new LocalCmdRunner(),
            file.getAbsolutePath(),
            SimpleLsfExecutorParameters.builder().build());
    assertThat(runnerResult.getStderr()).isEqualTo("test");
  }
}
