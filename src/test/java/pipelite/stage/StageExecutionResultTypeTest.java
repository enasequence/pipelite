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

import org.junit.jupiter.api.Test;

public class StageExecutionResultTypeTest {

  @Test
  public void test() {
    assertThat(StageState.NEW.isNew()).isTrue();
    assertThat(StageState.ACTIVE.isNew()).isFalse();
    assertThat(StageState.SUCCESS.isNew()).isFalse();
    assertThat(StageState.ERROR.isNew()).isFalse();

    assertThat(StageState.NEW.isActive()).isFalse();
    assertThat(StageState.ACTIVE.isActive()).isTrue();
    assertThat(StageState.SUCCESS.isActive()).isFalse();
    assertThat(StageState.ERROR.isActive()).isFalse();

    assertThat(StageState.NEW.isSuccess()).isFalse();
    assertThat(StageState.ACTIVE.isSuccess()).isFalse();
    assertThat(StageState.SUCCESS.isSuccess()).isTrue();
    assertThat(StageState.ERROR.isSuccess()).isFalse();

    assertThat(StageState.NEW.isError()).isFalse();
    assertThat(StageState.ACTIVE.isError()).isFalse();
    assertThat(StageState.SUCCESS.isError()).isFalse();
    assertThat(StageState.ERROR.isError()).isTrue();
  }
}
