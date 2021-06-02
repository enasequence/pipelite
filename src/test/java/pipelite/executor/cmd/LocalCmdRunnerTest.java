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
package pipelite.executor.cmd;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import pipelite.stage.executor.StageExecutorResult;
import pipelite.stage.executor.StageExecutorResultAttribute;
import pipelite.stage.parameters.CmdExecutorParameters;

public class LocalCmdRunnerTest {

  @Test
  public void writeFile() throws Exception {
    LocalCmdRunner cmdRunner = new LocalCmdRunner(CmdExecutorParameters.builder().build());
    String str = "test";
    Path tempFile = Files.createTempFile("test", "'test");
    cmdRunner.writeFile(str, tempFile);
    assertThat(new String(Files.readAllBytes(tempFile))).isEqualTo(str);
  }

  @Test
  public void deleteFile() throws Exception {
    LocalCmdRunner cmdRunner = new LocalCmdRunner(CmdExecutorParameters.builder().build());
    Path tempFile = Files.createTempFile("test", "'test");
    assertThat(tempFile).exists();
    cmdRunner.deleteFile(tempFile);
    assertThat(tempFile).doesNotExist();
  }

  @Test
  public void error() {
    LocalCmdRunner cmdRunner = new LocalCmdRunner(CmdExecutorParameters.builder().build());
    StageExecutorResult result = cmdRunner.execute("date");
    assertThat(result.isError()).isFalse();
    assertThat(result.getAttribute(StageExecutorResultAttribute.EXIT_CODE)).isEqualTo("0");
  }

  @Test
  public void permanentError() {
    LocalCmdRunner cmdRunner =
        new LocalCmdRunner(CmdExecutorParameters.builder().permanentError(0).build());
    StageExecutorResult result = cmdRunner.execute("date");
    assertThat(result.isError()).isTrue();
    assertThat(result.isPermanentError()).isTrue();
    assertThat(result.getAttribute(StageExecutorResultAttribute.EXIT_CODE)).isEqualTo("0");
  }
}
