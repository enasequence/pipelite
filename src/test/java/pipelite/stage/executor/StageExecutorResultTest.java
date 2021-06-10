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
    assertThat(StageExecutorResult.isExecutableErrorType(ErrorType.PERMANENT_ERROR.name()))
        .isFalse();
    assertThat(StageExecutorResult.isExecutableErrorType(ErrorType.TIMEOUT_ERROR.name())).isFalse();
    assertThat(StageExecutorResult.isExecutableErrorType(ErrorType.INTERNAL_ERROR.name())).isTrue();
    assertThat(StageExecutorResult.isExecutableErrorType(ErrorType.INTERRUPTED_ERROR.name()))
        .isTrue();
    assertThat(StageExecutorResult.isExecutableErrorType("TEST")).isTrue();
    assertThat(StageExecutorResult.isExecutableErrorType(null)).isTrue();
  }

  @Test
  public void getAttributesAsJson() {
    StageExecutorResult result = StageExecutorResult.success();
    result.addAttribute(StageExecutorResultAttribute.HOST, "test");
    assertThat(result.attributesJson()).isEqualTo("{\n" + "  \"host\" : \"test\"\n" + "}");
  }
}
