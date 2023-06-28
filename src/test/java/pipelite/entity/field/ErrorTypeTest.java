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

import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;
import pipelite.stage.executor.StageExecutorResult;
import pipelite.stage.executor.StageExecutorResultAttribute;
import pipelite.stage.executor.StageExecutorState;

public class ErrorTypeTest {

  @Test
  public void fromActive() {
    assertThat(ErrorType.from(StageExecutorResult.submitted(), Collections.emptyList())).isNull();
    assertThat(ErrorType.from(StageExecutorResult.active(), Collections.emptyList())).isNull();
  }

  @Test
  public void fromSuccess() {
    assertThat(ErrorType.from(StageExecutorResult.success(), Collections.emptyList())).isNull();
  }

  @Test
  public void fromNonPermanentError() {
    assertThat(
            ErrorType.from(
                StageExecutorResult.create(StageExecutorState.EXECUTION_ERROR),
                Collections.emptyList()))
        .isEqualTo(ErrorType.EXECUTION_ERROR);
    assertThat(
            ErrorType.from(
                StageExecutorResult.create(StageExecutorState.TIMEOUT_ERROR),
                Collections.emptyList()))
        .isEqualTo(ErrorType.TIMEOUT_ERROR);
    assertThat(
            ErrorType.from(
                StageExecutorResult.create(StageExecutorState.MEMORY_ERROR),
                Collections.emptyList()))
        .isEqualTo(ErrorType.MEMORY_ERROR);
    assertThat(
            ErrorType.from(
                StageExecutorResult.create(StageExecutorState.TERMINATED_ERROR),
                Collections.emptyList()))
        .isEqualTo(ErrorType.TERMINATED_ERROR);
    assertThat(
            ErrorType.from(
                StageExecutorResult.create(StageExecutorState.LOST_ERROR), Collections.emptyList()))
        .isEqualTo(ErrorType.LOST_ERROR);
    assertThat(
            ErrorType.from(
                StageExecutorResult.create(StageExecutorState.INTERNAL_ERROR),
                Collections.emptyList()))
        .isEqualTo(ErrorType.INTERNAL_ERROR);
  }

  @Test
  public void fromPermanentError() {
    StageExecutorResult result = StageExecutorResult.create(StageExecutorState.EXECUTION_ERROR);
    result.attribute(StageExecutorResultAttribute.EXIT_CODE, "1");
    assertThat(ErrorType.from(result, List.of(1))).isEqualTo(ErrorType.PERMANENT_ERROR);
  }

  @Test
  public void isPermentError() {
    assertThat(ErrorType.PERMANENT_ERROR.isPermanentError()).isTrue();
    assertThat(ErrorType.TIMEOUT_ERROR.isPermanentError()).isTrue();
    assertThat(ErrorType.MEMORY_ERROR.isPermanentError()).isTrue();

    assertThat(ErrorType.TERMINATED_ERROR.isPermanentError()).isFalse();
    assertThat(ErrorType.EXECUTION_ERROR.isPermanentError()).isFalse();
    assertThat(ErrorType.INTERNAL_ERROR.isPermanentError()).isFalse();
    assertThat(ErrorType.LOST_ERROR.isPermanentError()).isFalse();
  }
}
