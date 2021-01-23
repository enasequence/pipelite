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
import static pipelite.stage.StageState.*;

import org.junit.jupiter.api.Test;

public class StageStateTest {
  @Test
  public void test() {
    assertThat(StageState.isActive(ACTIVE)).isTrue();
    assertThat(StageState.isActive(SUCCESS)).isFalse();
    assertThat(StageState.isActive(ERROR)).isFalse();
    assertThat(StageState.isActive(null)).isFalse();

    assertThat(StageState.isSuccess(ACTIVE)).isFalse();
    assertThat(StageState.isSuccess(SUCCESS)).isTrue();
    assertThat(StageState.isSuccess(ERROR)).isFalse();
    assertThat(StageState.isSuccess(null)).isFalse();

    assertThat(StageState.isError(ACTIVE)).isFalse();
    assertThat(StageState.isError(SUCCESS)).isFalse();
    assertThat(StageState.isError(ERROR)).isTrue();
    assertThat(StageState.isError(null)).isFalse();
  }
}
