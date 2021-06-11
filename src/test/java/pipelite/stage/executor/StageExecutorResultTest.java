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

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class StageExecutorResultTest {

  @Test
  public void isSubmitted() {
    assertThat(new StageExecutorResult(StageExecutorState.SUBMITTED).isSubmitted()).isTrue();
  }

  @Test
  public void isError() {
    assertThat(new StageExecutorResult(StageExecutorState.ERROR).isError()).isTrue();
  }

  @Test
  public void isSuccess() {
    assertThat(new StageExecutorResult(StageExecutorState.SUCCESS).isSuccess()).isTrue();
  }

  @Test
  public void submitted() {
    assertThat(StageExecutorResult.submitted().getExecutorState())
        .isEqualTo(StageExecutorState.SUBMITTED);
  }

  @Test
  public void active() {
    assertThat(StageExecutorResult.active().getExecutorState())
        .isEqualTo(StageExecutorState.ACTIVE);
  }

  @Test
  public void error() {
    assertThat(StageExecutorResult.error().getExecutorState()).isEqualTo(StageExecutorState.ERROR);
  }

  @Test
  public void success() {
    assertThat(StageExecutorResult.success().getExecutorState())
        .isEqualTo(StageExecutorState.SUCCESS);
  }

  @Test
  public void setSubmitted() {
    assertThat(StageExecutorResult.success().setSubmitted().getExecutorState())
        .isEqualTo(StageExecutorState.SUBMITTED);
  }

  @Test
  public void isExecutableErrorType() {
    assertThat(StageExecutorResult.isExecutableErrorType(ErrorType.PERMANENT_ERROR)).isFalse();
    assertThat(StageExecutorResult.isExecutableErrorType(ErrorType.TIMEOUT_ERROR)).isFalse();
    assertThat(StageExecutorResult.isExecutableErrorType(ErrorType.INTERNAL_ERROR)).isTrue();
    assertThat(StageExecutorResult.isExecutableErrorType(ErrorType.INTERRUPTED_ERROR)).isTrue();
    assertThat(StageExecutorResult.isExecutableErrorType(null)).isTrue();
  }

  @Test
  public void getErrorType() {
    // Set and check error type using static factory methods.
    assertThat(StageExecutorResult.submitted().getErrorType()).isNull();
    assertThat(StageExecutorResult.active().getErrorType()).isNull();
    assertThat(StageExecutorResult.success().getErrorType()).isNull();
    assertThat(StageExecutorResult.error().getErrorType()).isEqualTo(ErrorType.EXECUTION_ERROR);
    assertThat(StageExecutorResult.internalError(new RuntimeException()).getErrorType())
        .isEqualTo(ErrorType.INTERNAL_ERROR);
    assertThat(StageExecutorResult.timeoutError().getErrorType())
        .isEqualTo(ErrorType.TIMEOUT_ERROR);
    assertThat(StageExecutorResult.interruptedError().getErrorType())
        .isEqualTo(ErrorType.INTERRUPTED_ERROR);
    assertThat(StageExecutorResult.permanentError().getErrorType())
        .isEqualTo(ErrorType.PERMANENT_ERROR);

    // Set and check error type using set*Error.
    assertThat(StageExecutorResult.submitted().setInternalError().getErrorType())
        .isEqualTo(ErrorType.INTERNAL_ERROR);
    assertThat(StageExecutorResult.submitted().setTimeoutError().getErrorType())
        .isEqualTo(ErrorType.TIMEOUT_ERROR);
    assertThat(StageExecutorResult.submitted().setInterruptedError().getErrorType())
        .isEqualTo(ErrorType.INTERRUPTED_ERROR);
    assertThat(StageExecutorResult.submitted().setPermanentError().getErrorType())
        .isEqualTo(ErrorType.PERMANENT_ERROR);

    // Set and check error type using setErrorType.
    for (ErrorType errorType : ErrorType.values()) {
      assertThat(StageExecutorResult.submitted().setErrorType(errorType).getErrorType())
          .isEqualTo(errorType);
    }

    // Set error type to null after setting it using setErrorType.
    // Error type should be the default EXECUTION_ERROR.
    for (ErrorType errorType : ErrorType.values()) {
      assertThat(
              StageExecutorResult.submitted()
                  .setErrorType(errorType)
                  .setErrorType(null)
                  .getErrorType())
          .isEqualTo(ErrorType.EXECUTION_ERROR);
    }
  }

  @Test
  public void getAttributesAsJson() {
    StageExecutorResult result = StageExecutorResult.success();
    result.addAttribute(StageExecutorResultAttribute.HOST, "test");
    assertThat(result.attributesJson()).isEqualTo("{\n" + "  \"host\" : \"test\"\n" + "}");
  }
}
