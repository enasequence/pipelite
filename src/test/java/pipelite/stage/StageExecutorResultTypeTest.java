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
package pipelite.stage;

import static org.assertj.core.api.Assertions.assertThat;
import static pipelite.stage.executor.StageExecutorResultType.*;

import org.junit.jupiter.api.Test;
import pipelite.stage.executor.StageExecutorResultType;

public class StageExecutorResultTypeTest {
  @Test
  public void test() {
    assertThat(StageExecutorResultType.isActive(ACTIVE)).isTrue();
    assertThat(StageExecutorResultType.isActive(SUCCESS)).isFalse();
    assertThat(StageExecutorResultType.isActive(ERROR)).isFalse();
    assertThat(StageExecutorResultType.isActive(null)).isFalse();

    assertThat(StageExecutorResultType.isSuccess(ACTIVE)).isFalse();
    assertThat(StageExecutorResultType.isSuccess(SUCCESS)).isTrue();
    assertThat(StageExecutorResultType.isSuccess(ERROR)).isFalse();
    assertThat(StageExecutorResultType.isSuccess(null)).isFalse();

    assertThat(StageExecutorResultType.isError(ACTIVE)).isFalse();
    assertThat(StageExecutorResultType.isError(SUCCESS)).isFalse();
    assertThat(StageExecutorResultType.isError(ERROR)).isTrue();
    assertThat(StageExecutorResultType.isError(null)).isFalse();
  }
}
