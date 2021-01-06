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

import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

public class LocalCmdRunnerTest {

  @Test
  public void writeFile() throws Exception {
    LocalCmdRunner cmdRunner = new LocalCmdRunner();
    String str = "test";
    Path tempFile = Files.createTempFile("test", "'test");
    cmdRunner.writeFile(str, tempFile, null);
    assertThat(new String(Files.readAllBytes(tempFile))).isEqualTo(str);
  }

  @Test
  public void deleteFile() throws Exception {
    LocalCmdRunner cmdRunner = new LocalCmdRunner();
    Path tempFile = Files.createTempFile("test", "'test");
    assertThat(tempFile).exists();
    cmdRunner.deleteFile(tempFile, null);
    assertThat(tempFile).doesNotExist();
  }
}
