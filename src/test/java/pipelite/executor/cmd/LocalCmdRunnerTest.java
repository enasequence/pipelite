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
package pipelite.executor.cmd;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import pipelite.stage.executor.StageExecutorResult;
import pipelite.stage.executor.StageExecutorResultAttribute;
import pipelite.stage.parameters.CmdExecutorParameters;
import pipelite.test.PipeliteTestIdCreator;

public class LocalCmdRunnerTest {

  private static final String TMP_DIR = "/tmp";

  @Test
  public void writeFileAndReadFile() {
    LocalCmdRunner cmdRunner = new LocalCmdRunner(CmdExecutorParameters.builder().build());
    String str = "test";

    // Create file in temp dir
    String tempFile = cmdRunner.createTempFile();
    assertThat(cmdRunner.fileExists(Path.of(tempFile))).isTrue();

    // Write file
    cmdRunner.writeFile(str, Path.of(tempFile));

    // Read file
    assertThat(cmdRunner.readFile(Path.of(tempFile), 10)).isEqualTo("test");
  }

  @Test
  public void createFileAndFileExistsAndDeleteFile() {
    LocalCmdRunner cmdRunner = new LocalCmdRunner(CmdExecutorParameters.builder().build());

    // Create file in temp dir
    String tempFile = cmdRunner.createTempFile();
    assertThat(cmdRunner.fileExists(Path.of(tempFile))).isTrue();

    // Delete file
    cmdRunner.deleteFile(Path.of(tempFile));
    assertThat(cmdRunner.fileExists(Path.of(tempFile))).isFalse();
  }

  @Test
  public void echo() {
    LocalCmdRunner cmdRunner = new LocalCmdRunner(CmdExecutorParameters.builder().build());
    StageExecutorResult result = cmdRunner.execute("echo test");
    assertThat(result.isError()).isFalse();
    assertThat(result.attribute(StageExecutorResultAttribute.EXIT_CODE)).isEqualTo("0");
    assertThat(result.stdOut()).startsWith("test");
    assertThat(result.stageLog()).startsWith("test");
  }

  @Test
  public void unknownCommand() {
    LocalCmdRunner cmdRunner = new LocalCmdRunner(CmdExecutorParameters.builder().build());
    StageExecutorResult result = cmdRunner.execute(PipeliteTestIdCreator.id());
    assertThat(result.isError()).isTrue();
    assertThat(result.attribute(StageExecutorResultAttribute.EXIT_CODE)).isNotEqualTo("0");
  }
}
