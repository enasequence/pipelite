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

import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;

public class CmdRunnerUtilsTest {

  @Test
  public void testQuoteArgument() {
    assertThat(CmdRunnerUtils.quoteArgument("test")).isEqualTo("'test'");
    assertThat(CmdRunnerUtils.quoteArgument("'test'")).isEqualTo("'test'");
    assertThat(CmdRunnerUtils.quoteArgument("-i")).isEqualTo("-i");
  }

  @Test
  public void testQuoteArguments() {
    List<String> arguments = Arrays.asList("arg1", "arg2", "-i");
    assertThat(CmdRunnerUtils.quoteArguments(arguments)).isEqualTo("'arg1' 'arg2' -i");
  }
}
