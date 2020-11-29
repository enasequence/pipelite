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
import static pipelite.stage.StageExecutionResultType.*;

import org.junit.jupiter.api.Test;

public class StageExecutionResultTypeTest {
  @Test
  public void test() {
    assertThat(StageExecutionResultType.isActive(ACTIVE)).isTrue();
    assertThat(StageExecutionResultType.isActive(SUCCESS)).isFalse();
    assertThat(StageExecutionResultType.isActive(ERROR)).isFalse();
    assertThat(StageExecutionResultType.isActive(null)).isFalse();

    assertThat(StageExecutionResultType.isSuccess(ACTIVE)).isFalse();
    assertThat(StageExecutionResultType.isSuccess(SUCCESS)).isTrue();
    assertThat(StageExecutionResultType.isSuccess(ERROR)).isFalse();
    assertThat(StageExecutionResultType.isSuccess(null)).isFalse();

    assertThat(StageExecutionResultType.isError(ACTIVE)).isFalse();
    assertThat(StageExecutionResultType.isError(SUCCESS)).isFalse();
    assertThat(StageExecutionResultType.isError(ERROR)).isTrue();
    assertThat(StageExecutionResultType.isError(null)).isFalse();
  }
}
