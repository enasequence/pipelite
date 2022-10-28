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
package pipelite.executor.describe;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

import org.junit.jupiter.api.Test;
import pipelite.exception.PipeliteException;
import pipelite.executor.describe.context.request.DefaultRequestContext;
import pipelite.stage.executor.StageExecutorResult;
import pipelite.stage.executor.StageExecutorResultAttribute;
import pipelite.stage.executor.StageExecutorState;

public class DescribeJobsResultTest {

  private final StageExecutorResult result = StageExecutorResult.success();

  private final String validJobId = "validJobId";

  private final DefaultRequestContext request = new DefaultRequestContext(validJobId);

  @Test
  public void create() {
    assertThat(DescribeJobsResult.create(request, null).request).isSameAs(request);
    assertThat(DescribeJobsResult.create(request, null).result).isNull();

    assertThat(DescribeJobsResult.create(request, result).request).isSameAs(request);
    assertThat(DescribeJobsResult.create(request, result).result).isSameAs(result);

    assertThatThrownBy(() -> DescribeJobsResult.create(null, null))
        .isInstanceOf(PipeliteException.class);
    assertThatThrownBy(() -> DescribeJobsResult.create(null, result))
        .isInstanceOf(PipeliteException.class);
  }

  @Test
  public void build() {
    assertThat(DescribeJobsResult.builder(request).unknown().build().request).isSameAs(request);
    assertThat(DescribeJobsResult.builder(request).active().build().request).isSameAs(request);
    assertThat(DescribeJobsResult.builder(request).executionError().build().request)
        .isSameAs(request);
    assertThat(DescribeJobsResult.builder(request).success().build().request).isSameAs(request);

    assertThat(DescribeJobsResult.builder(request).unknown().build().result).isNull();
    assertThat(DescribeJobsResult.builder(request).active().build().result.state())
        .isSameAs(StageExecutorState.ACTIVE);
    assertThat(DescribeJobsResult.builder(request).executionError().build().result.state())
        .isSameAs(StageExecutorState.EXECUTION_ERROR);
    assertThat(DescribeJobsResult.builder(request).success().build().result.state())
        .isSameAs(StageExecutorState.SUCCESS);

    assertThat(DescribeJobsResult.builder(request).unknown().isCompleted()).isFalse();
    assertThat(DescribeJobsResult.builder(request).active().isCompleted()).isFalse();
    assertThat(DescribeJobsResult.builder(request).executionError().isCompleted()).isTrue();
    assertThat(DescribeJobsResult.builder(request).success().isCompleted()).isTrue();

    assertThat(DescribeJobsResult.builder(request).unknown().build().result).isNull();
    assertThat(
            DescribeJobsResult.builder(request)
                .active()
                .build()
                .result
                .attribute(StageExecutorResultAttribute.EXIT_CODE))
        .isNull();
    assertThat(
            DescribeJobsResult.builder(request)
                .executionError()
                .build()
                .result
                .attribute(StageExecutorResultAttribute.EXIT_CODE))
        .isNull();
    assertThat(
            DescribeJobsResult.builder(request)
                .executionError(1)
                .build()
                .result
                .attribute(StageExecutorResultAttribute.EXIT_CODE))
        .isEqualTo("1");
    assertThat(
            DescribeJobsResult.builder(request)
                .success()
                .build()
                .result
                .attribute(StageExecutorResultAttribute.EXIT_CODE))
        .isEqualTo("0");

    assertThat(
            DescribeJobsResult.builder(request).unknown().attribute("test", "test").build().result)
        .isNull();
    assertThat(
            DescribeJobsResult.builder(request)
                .active()
                .attribute("test", "test")
                .build()
                .result
                .attribute("test"))
        .isEqualTo("test");
    assertThat(
            DescribeJobsResult.builder(request)
                .executionError()
                .attribute("test", "test")
                .build()
                .result
                .attribute("test"))
        .isEqualTo("test");
    assertThat(
            DescribeJobsResult.builder(request)
                .success()
                .attribute("test", "test")
                .build()
                .result
                .attribute("test"))
        .isEqualTo("test");
  }
}
