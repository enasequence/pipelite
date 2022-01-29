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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;
import pipelite.stage.parameters.ExecutorParametersValidator;

public class CmdRunnerUtilsTest {

  @Test
  public void quoteArgument() {
    assertThat(CmdRunnerUtils.quoteArgument("test")).isEqualTo("'test'");
    assertThat(CmdRunnerUtils.quoteArgument("'test'")).isEqualTo("'test'");
    assertThat(CmdRunnerUtils.quoteArgument("-i")).isEqualTo("-i");
  }

  @Test
  public void quoteArguments() {
    List<String> arguments = Arrays.asList("arg1", "arg2", "-i");
    assertThat(CmdRunnerUtils.quoteArguments(arguments)).isEqualTo("'arg1' 'arg2' -i");
  }

  @Test
  public void readUrl() {
    // https

    assertThat(
            CmdRunnerUtils.read(
                ExecutorParametersValidator.validateUrl("https://www.ebi.ac.uk", "test")))
        .contains("EMBL-EBI");

    // resource

    String text =
        "resource:\n"
            + "  n: 1\n"
            + "  R: rusage[mem=100M:duration=1]\n"
            + "limit:\n"
            + "  M: 100M\n"
            + "command: echo test\n";
    assertThat(
            CmdRunnerUtils.read(
                ExecutorParametersValidator.validateUrl("pipelite/executor/lsf.yaml", "test")))
        .isEqualTo(text);
  }

  @Test
  public void write() throws IOException {
    String str = "test";
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    CmdRunnerUtils.write(str, out);
    assertThat(out.toString()).isEqualTo(str);
  }
}
