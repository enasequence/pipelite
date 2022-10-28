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

public class ErrorTypeTest {

  @Test
  public void from() {
    assertThat(ErrorType.from(StageExecutorResult.submitted(), Collections.emptyList())).isNull();
    assertThat(ErrorType.from(StageExecutorResult.active(), Collections.emptyList())).isNull();
    assertThat(ErrorType.from(StageExecutorResult.success(), Collections.emptyList())).isNull();

    // Execution error
    assertThat(ErrorType.from(StageExecutorResult.executionError(), Collections.emptyList()))
        .isEqualTo(ErrorType.EXECUTION_ERROR);

    // Permanent error
    StageExecutorResult permanentError = StageExecutorResult.executionError();
    permanentError.attribute(StageExecutorResultAttribute.EXIT_CODE, "1");
    assertThat(ErrorType.from(permanentError, List.of(1))).isEqualTo(ErrorType.PERMANENT_ERROR);

    // Internal error
    assertThat(ErrorType.from(StageExecutorResult.internalError(), Collections.emptyList()))
        .isEqualTo(ErrorType.INTERNAL_ERROR);

    // Timeout error
    assertThat(ErrorType.from(StageExecutorResult.timeoutError(), Collections.emptyList()))
        .isEqualTo(ErrorType.TIMEOUT_ERROR);

    // Lost error
    assertThat(ErrorType.from(StageExecutorResult.lostError(), Collections.emptyList()))
        .isEqualTo(ErrorType.LOST_ERROR);
  }
}
