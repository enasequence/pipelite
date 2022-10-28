/*
 * Copyright 2020-2022 EMBL - European Bioinformatics Institute
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

public class ErrorTypeTest {
  @Test
  public void fromStageExecutorResult() {
    assertThat(ErrorType.from(StageExecutorResult.submitted())).isNull();
    assertThat(ErrorType.from(StageExecutorResult.active())).isNull();
    assertThat(ErrorType.from(StageExecutorResult.success())).isNull();
    assertThat(ErrorType.from(StageExecutorResult.executionError()))
        .isEqualTo(ErrorType.EXECUTION_ERROR);
    assertThat(ErrorType.from(StageExecutorResult.permanentError()))
        .isEqualTo(ErrorType.PERMANENT_ERROR);
    assertThat(ErrorType.from(StageExecutorResult.internalError()))
        .isEqualTo(ErrorType.INTERNAL_ERROR);
    assertThat(ErrorType.from(StageExecutorResult.timeoutError()))
        .isEqualTo(ErrorType.TIMEOUT_ERROR);
    assertThat(ErrorType.from(StageExecutorResult.lostError())).isEqualTo(ErrorType.LOST_ERROR);
  }

  @Test
  public void fromStageExecutorState() {
    assertThat(ErrorType.from(StageExecutorState.SUBMITTED)).isNull();
    assertThat(ErrorType.from(StageExecutorState.ACTIVE)).isNull();
    assertThat(ErrorType.from(StageExecutorState.SUCCESS)).isNull();
    assertThat(ErrorType.from(StageExecutorState.EXECUTION_ERROR))
        .isEqualTo(ErrorType.EXECUTION_ERROR);
    assertThat(ErrorType.from(StageExecutorState.PERMANENT_ERROR))
        .isEqualTo(ErrorType.PERMANENT_ERROR);
    assertThat(ErrorType.from(StageExecutorState.INTERNAL_ERROR))
        .isEqualTo(ErrorType.INTERNAL_ERROR);
    assertThat(ErrorType.from(StageExecutorState.TIMEOUT_ERROR)).isEqualTo(ErrorType.TIMEOUT_ERROR);
    assertThat(ErrorType.from(StageExecutorState.LOST_ERROR)).isEqualTo(ErrorType.LOST_ERROR);
  }
}
