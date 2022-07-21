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

import java.util.Arrays;
import org.junit.jupiter.api.Test;
import pipelite.entity.field.ErrorType;

public class StageExecutorResultTest {

  @Test
  public void isSubmitted() {
    StageExecutorResult result = StageExecutorResult.submitted();
    assertThat(result.isSubmitted()).isTrue();
    assertThat(result.isActive()).isFalse();
    assertThat(result.isSuccess()).isFalse();
    assertThat(result.isError()).isFalse();
  }

  @Test
  public void isActive() {
    StageExecutorResult result = StageExecutorResult.active();
    assertThat(result.isSubmitted()).isFalse();
    assertThat(result.isActive()).isTrue();
    assertThat(result.isSuccess()).isFalse();
    assertThat(result.isError()).isFalse();
  }

  @Test
  public void isSuccess() {
    StageExecutorResult result = StageExecutorResult.success();
    assertThat(result.isSubmitted()).isFalse();
    assertThat(result.isActive()).isFalse();
    assertThat(result.isSuccess()).isTrue();
    assertThat(result.isError()).isFalse();
  }

  @Test
  public void isError() {
    for (StageExecutorResult result :
        Arrays.asList(
            StageExecutorResult.executionError(),
            StageExecutorResult.timeoutError(),
            StageExecutorResult.permanentError(),
            StageExecutorResult.internalError())) {
      assertThat(result.isSubmitted()).isFalse();
      assertThat(result.isActive()).isFalse();
      assertThat(result.isSuccess()).isFalse();
      assertThat(result.isError()).isTrue();
    }
  }

  @Test
  public void state() {
    for (StageExecutorState stateFrom : StageExecutorState.values()) {
      for (StageExecutorState stateTo : StageExecutorState.values()) {
        StageExecutorResult resultFrom = StageExecutorResult.create(stateFrom);
        assertThat(resultFrom.state()).isSameAs(stateFrom);
        StageExecutorResult resultTo = resultFrom.state(stateTo);
        assertThat(resultTo.state()).isSameAs(stateTo);
      }
    }
  }

  @Test
  public void create() {
    assertThat(StageExecutorResult.create(StageExecutorState.SUBMITTED).state())
        .isSameAs(StageExecutorState.SUBMITTED);
    assertThat(StageExecutorResult.create(StageExecutorState.ACTIVE).state())
        .isSameAs(StageExecutorState.ACTIVE);
    assertThat(StageExecutorResult.create(StageExecutorState.SUCCESS).state())
        .isSameAs(StageExecutorState.SUCCESS);
    assertThat(StageExecutorResult.create(StageExecutorState.EXECUTION_ERROR).state())
        .isSameAs(StageExecutorState.EXECUTION_ERROR);
    assertThat(StageExecutorResult.create(StageExecutorState.TIMEOUT_ERROR).state())
        .isSameAs(StageExecutorState.TIMEOUT_ERROR);
    assertThat(StageExecutorResult.create(StageExecutorState.PERMANENT_ERROR).state())
        .isSameAs(StageExecutorState.PERMANENT_ERROR);
    assertThat(StageExecutorResult.create(StageExecutorState.INTERNAL_ERROR).state())
        .isSameAs(StageExecutorState.INTERNAL_ERROR);
  }

  @Test
  public void createFromMethod() {
    assertThat(StageExecutorResult.submitted().state()).isSameAs(StageExecutorState.SUBMITTED);
    assertThat(StageExecutorResult.active().state()).isSameAs(StageExecutorState.ACTIVE);
    assertThat(StageExecutorResult.success().state()).isSameAs(StageExecutorState.SUCCESS);
    assertThat(StageExecutorResult.executionError().state())
        .isSameAs(StageExecutorState.EXECUTION_ERROR);
    assertThat(StageExecutorResult.timeoutError().state())
        .isSameAs(StageExecutorState.TIMEOUT_ERROR);
    assertThat(StageExecutorResult.permanentError().state())
        .isSameAs(StageExecutorState.PERMANENT_ERROR);
    assertThat(StageExecutorResult.internalError().state())
        .isSameAs(StageExecutorState.INTERNAL_ERROR);
  }

  @Test
  public void createFromErrorType() {
    assertThat(StageExecutorResult.create(ErrorType.EXECUTION_ERROR).state())
        .isEqualTo(StageExecutorState.EXECUTION_ERROR);
    assertThat(StageExecutorResult.create(ErrorType.INTERNAL_ERROR).state())
        .isEqualTo(StageExecutorState.INTERNAL_ERROR);
    assertThat(StageExecutorResult.create(ErrorType.TIMEOUT_ERROR).state())
        .isEqualTo(StageExecutorState.TIMEOUT_ERROR);
    assertThat(StageExecutorResult.create(ErrorType.PERMANENT_ERROR).state())
        .isEqualTo(StageExecutorState.PERMANENT_ERROR);
  }

  @Test
  public void getAttributesAsJson() {
    StageExecutorResult result = StageExecutorResult.success();
    result.attribute(StageExecutorResultAttribute.HOST, "test");
    assertThat(result.attributesJson()).isEqualTo("{\n" + "  \"host\" : \"test\"\n" + "}");
  }
}
