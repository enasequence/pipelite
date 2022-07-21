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
package pipelite.stage.executor;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

public class StageExecutorResultTest {

  @Test
  public void submitted() {
    StageExecutorResult result = StageExecutorResult.submitted();
    assertThat(result.isSubmitted()).isTrue();
    assertThat(result.isActive()).isFalse();
    assertThat(result.isSuccess()).isFalse();
    assertThat(result.isError()).isFalse();
    assertThat(result.state()).isEqualTo(StageExecutorState.SUBMITTED);
  }

  @Test
  public void active() {
    StageExecutorResult result = StageExecutorResult.active();
    assertThat(result.isSubmitted()).isFalse();
    assertThat(result.isActive()).isTrue();
    assertThat(result.isSuccess()).isFalse();
    assertThat(result.isError()).isFalse();
    assertThat(result.state()).isEqualTo(StageExecutorState.ACTIVE);
  }

  @Test
  public void isSuccess() {
    StageExecutorResult result = StageExecutorResult.success();
    assertThat(result.isSubmitted()).isFalse();
    assertThat(result.isActive()).isFalse();
    assertThat(result.isSuccess()).isTrue();
    assertThat(result.isError()).isFalse();
    assertThat(result.state()).isEqualTo(StageExecutorState.SUCCESS);
  }

  @Test
  public void isError() {
    StageExecutorResult result = StageExecutorResult.executionError();
    assertThat(result.isSubmitted()).isFalse();
    assertThat(result.isActive()).isFalse();
    assertThat(result.isSuccess()).isFalse();
    assertThat(result.isError()).isTrue();
    assertThat(result.state()).isEqualTo(StageExecutorState.ERROR);
  }

  @Test
  public void getErrorType() {
    // Set and check error type using static factory methods.
    assertThat(StageExecutorResult.submitted().errorType()).isNull();
    assertThat(StageExecutorResult.active().errorType()).isNull();
    assertThat(StageExecutorResult.success().errorType()).isNull();

    assertThat(StageExecutorResult.executionError().errorType())
        .isEqualTo(ErrorType.EXECUTION_ERROR);
    assertThat(StageExecutorResult.internalError().errorType()).isEqualTo(ErrorType.INTERNAL_ERROR);
    assertThat(StageExecutorResult.timeoutError().errorType()).isEqualTo(ErrorType.TIMEOUT_ERROR);
    assertThat(StageExecutorResult.permanentError().errorType())
        .isEqualTo(ErrorType.PERMANENT_ERROR);

    // Set and check error type using errorType method.
    assertThat(StageExecutorResult.submitted().errorType(ErrorType.EXECUTION_ERROR).errorType())
        .isEqualTo(ErrorType.EXECUTION_ERROR);
    assertThat(StageExecutorResult.submitted().errorType(ErrorType.INTERNAL_ERROR).errorType())
        .isEqualTo(ErrorType.INTERNAL_ERROR);
    assertThat(StageExecutorResult.submitted().errorType(ErrorType.TIMEOUT_ERROR).errorType())
        .isEqualTo(ErrorType.TIMEOUT_ERROR);
    assertThat(StageExecutorResult.submitted().errorType(ErrorType.PERMANENT_ERROR).errorType())
        .isEqualTo(ErrorType.PERMANENT_ERROR);
    assertThat(StageExecutorResult.submitted().errorType(ErrorType.EXECUTION_ERROR).state())
        .isEqualTo(StageExecutorState.ERROR);
    assertThat(StageExecutorResult.submitted().errorType(ErrorType.INTERNAL_ERROR).state())
        .isEqualTo(StageExecutorState.ERROR);
    assertThat(StageExecutorResult.submitted().errorType(ErrorType.TIMEOUT_ERROR).state())
        .isEqualTo(StageExecutorState.ERROR);
    assertThat(StageExecutorResult.submitted().errorType(ErrorType.PERMANENT_ERROR).state())
        .isEqualTo(StageExecutorState.ERROR);

    // Set and check error type using error with errorType.
    for (ErrorType errorType : ErrorType.values()) {
      assertThat(StageExecutorResult.error(errorType).errorType()).isEqualTo(errorType);
    }
  }

  @Test
  public void getAttributesAsJson() {
    StageExecutorResult result = StageExecutorResult.success();
    result.attribute(StageExecutorResultAttribute.HOST, "test");
    assertThat(result.attributesJson()).isEqualTo("{\n" + "  \"host\" : \"test\"\n" + "}");
  }
}
