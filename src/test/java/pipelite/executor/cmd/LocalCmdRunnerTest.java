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

import java.nio.file.Path;
import java.nio.file.Paths;
import org.junit.jupiter.api.Test;
import pipelite.PipeliteIdCreator;
import pipelite.stage.executor.StageExecutorResult;
import pipelite.stage.executor.StageExecutorResultAttribute;
import pipelite.stage.parameters.CmdExecutorParameters;

public class LocalCmdRunnerTest {

  private static final String TMP_DIR = "/tmp";

  @Test
  public void writeFileAndReadFile() {
    LocalCmdRunner cmdRunner = new LocalCmdRunner(CmdExecutorParameters.builder().build());
    String str = "test";

    // Create file in temp dir
    String fileName = PipeliteIdCreator.id();
    Path file = Paths.get(TMP_DIR, fileName);
    assertThat(cmdRunner.fileExists(file)).isFalse();
    cmdRunner.createFile(file);
    assertThat(cmdRunner.fileExists(file)).isTrue();

    // Write file
    cmdRunner.writeFile(str, file);

    // Read file
    assertThat(cmdRunner.readFile(file, 10)).isEqualTo("test");
  }

  @Test
  public void createFileAndFileExistsAndDeleteFile() {
    LocalCmdRunner cmdRunner = new LocalCmdRunner(CmdExecutorParameters.builder().build());

    // Create file in temp dir
    String fileName = PipeliteIdCreator.id();
    Path file = Paths.get(TMP_DIR, fileName);
    assertThat(cmdRunner.fileExists(file)).isFalse();
    cmdRunner.createFile(file);
    assertThat(cmdRunner.fileExists(file)).isTrue();

    // Delete file
    cmdRunner.deleteFile(file);
    assertThat(cmdRunner.fileExists(file)).isFalse();
  }

  @Test
  public void createDirAndDirExists() {
    LocalCmdRunner cmdRunner = new LocalCmdRunner(CmdExecutorParameters.builder().build());

    // Create directory in temp dir
    String dirName = PipeliteIdCreator.id();
    Path dir = Paths.get(TMP_DIR, dirName);
    assertThat(cmdRunner.dirExists(dir)).isFalse();
    cmdRunner.createDir(dir);
    assertThat(cmdRunner.dirExists(dir)).isTrue();
  }

  @Test
  public void echo() {
    LocalCmdRunner cmdRunner = new LocalCmdRunner(CmdExecutorParameters.builder().build());
    StageExecutorResult result = cmdRunner.execute("echo test");
    assertThat(result.isError()).isFalse();
    assertThat(result.getAttribute(StageExecutorResultAttribute.EXIT_CODE)).isEqualTo("0");
    assertThat(result.getStageLog()).startsWith("test");
  }

  @Test
  public void unknownCommand() {
    LocalCmdRunner cmdRunner = new LocalCmdRunner(CmdExecutorParameters.builder().build());
    StageExecutorResult result = cmdRunner.execute(PipeliteIdCreator.id());
    assertThat(result.isError()).isTrue();
    assertThat(result.getAttribute(StageExecutorResultAttribute.EXIT_CODE)).isNotEqualTo("0");
  }
}
