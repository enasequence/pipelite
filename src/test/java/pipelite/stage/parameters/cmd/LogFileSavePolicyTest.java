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
package pipelite.stage.parameters.cmd;

import static org.assertj.core.api.Assertions.assertThat;
import static pipelite.stage.parameters.cmd.LogFileSavePolicy.*;

import org.junit.jupiter.api.Test;
import pipelite.stage.executor.StageExecutorResult;

public class LogFileSavePolicyTest {

  private StageExecutorResult error() {
    return StageExecutorResult.error();
  }

  private StageExecutorResult success() {
    return StageExecutorResult.success();
  }

  @Test
  public void test() {
    assertThat(isSave(ALWAYS, error())).isTrue();
    assertThat(isSave(ALWAYS, success())).isTrue();

    assertThat(isSave(ERROR, error())).isTrue();
    assertThat(isSave(ERROR, success())).isFalse();

    assertThat(isSave(NEVER, error())).isFalse();
    assertThat(isSave(NEVER, success())).isFalse();
  }
}
