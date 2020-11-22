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

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class StageExecutionResultTypeTest {

  @Test
  public void test() {
    assertThat(StageExecutionResultType.NEW.isNew()).isTrue();
    assertThat(StageExecutionResultType.ACTIVE.isNew()).isFalse();
    assertThat(StageExecutionResultType.SUCCESS.isNew()).isFalse();
    assertThat(StageExecutionResultType.ERROR.isNew()).isFalse();

    assertThat(StageExecutionResultType.NEW.isActive()).isFalse();
    assertThat(StageExecutionResultType.ACTIVE.isActive()).isTrue();
    assertThat(StageExecutionResultType.SUCCESS.isActive()).isFalse();
    assertThat(StageExecutionResultType.ERROR.isActive()).isFalse();

    assertThat(StageExecutionResultType.NEW.isSuccess()).isFalse();
    assertThat(StageExecutionResultType.ACTIVE.isSuccess()).isFalse();
    assertThat(StageExecutionResultType.SUCCESS.isSuccess()).isTrue();
    assertThat(StageExecutionResultType.ERROR.isSuccess()).isFalse();

    assertThat(StageExecutionResultType.NEW.isError()).isFalse();
    assertThat(StageExecutionResultType.ACTIVE.isError()).isFalse();
    assertThat(StageExecutionResultType.SUCCESS.isError()).isFalse();
    assertThat(StageExecutionResultType.ERROR.isError()).isTrue();
  }
}
