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
package pipelite.entity.field;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import pipelite.stage.executor.StageExecutorResult;
import pipelite.stage.executor.StageExecutorState;

public class StageStateTest {
  @Test
  public void fromStageExecutorResult() {
    assertThat(StageState.from(StageExecutorResult.submitted())).isEqualTo(StageState.ACTIVE);
    assertThat(StageState.from(StageExecutorResult.active())).isEqualTo(StageState.ACTIVE);
    assertThat(StageState.from(StageExecutorResult.success())).isEqualTo(StageState.SUCCESS);
    assertThat(StageState.from(StageExecutorResult.executionError())).isEqualTo(StageState.ERROR);
    assertThat(StageState.from(StageExecutorResult.permanentError())).isEqualTo(StageState.ERROR);
    assertThat(StageState.from(StageExecutorResult.internalError())).isEqualTo(StageState.ERROR);
    assertThat(StageState.from(StageExecutorResult.timeoutError())).isEqualTo(StageState.ERROR);
  }

  @Test
  public void fromStageExecutorState() {
    assertThat(StageState.from(StageExecutorState.SUBMITTED)).isEqualTo(StageState.ACTIVE);
    assertThat(StageState.from(StageExecutorState.ACTIVE)).isEqualTo(StageState.ACTIVE);
    assertThat(StageState.from(StageExecutorState.SUCCESS)).isEqualTo(StageState.SUCCESS);
    assertThat(StageState.from(StageExecutorState.EXECUTION_ERROR)).isEqualTo(StageState.ERROR);
    assertThat(StageState.from(StageExecutorState.PERMANENT_ERROR)).isEqualTo(StageState.ERROR);
    assertThat(StageState.from(StageExecutorState.INTERNAL_ERROR)).isEqualTo(StageState.ERROR);
    assertThat(StageState.from(StageExecutorState.TIMEOUT_ERROR)).isEqualTo(StageState.ERROR);
  }
}
