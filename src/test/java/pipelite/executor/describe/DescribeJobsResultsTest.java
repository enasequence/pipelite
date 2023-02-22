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

import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import pipelite.executor.describe.context.request.DefaultRequestContext;
import pipelite.stage.executor.StageExecutorState;

public class DescribeJobsResultsTest {

  private DefaultRequestContext request(String jobId) {
    return new DefaultRequestContext(jobId);
  }

  @Test
  public void test() {
    DescribeJobsResults<DefaultRequestContext> results = new DescribeJobsResults<>();

    DefaultRequestContext successRequest = request("1");
    DefaultRequestContext timeoutErrorRequest = request("2");
    DefaultRequestContext executionErrorRequest = request("3");
    DefaultRequestContext lostErrorRequest = request("4");

    results.add(DescribeJobsResult.builder(successRequest).success().build());
    results.add(DescribeJobsResult.builder(timeoutErrorRequest).timeoutError().build());
    results.add(DescribeJobsResult.builder(executionErrorRequest).executionError().build());
    results.add(DescribeJobsResult.builder(lostErrorRequest).lostError().build());

    assertThat(results.foundCount()).isEqualTo(3);
    assertThat(results.lostCount()).isEqualTo(1);

    assertThat(results.get().stream().map(r -> r.request).collect(Collectors.toList()))
        .containsExactly(
            successRequest, timeoutErrorRequest, executionErrorRequest, lostErrorRequest);
    assertThat(results.found().map(r -> r.request).collect(Collectors.toList()))
        .containsExactly(successRequest, timeoutErrorRequest, executionErrorRequest);
    assertThat(results.lost().map(r -> r.request).collect(Collectors.toList()))
        .containsExactly(lostErrorRequest);

    assertThat(results.get().stream().map(r -> r.result.state()).collect(Collectors.toList()))
        .containsExactly(
            StageExecutorState.SUCCESS,
            StageExecutorState.TIMEOUT_ERROR,
            StageExecutorState.EXECUTION_ERROR,
            StageExecutorState.LOST_ERROR);
    assertThat(results.found().map(r -> r.result.state()).collect(Collectors.toList()))
        .containsExactly(
            StageExecutorState.SUCCESS,
            StageExecutorState.TIMEOUT_ERROR,
            StageExecutorState.EXECUTION_ERROR);
    assertThat(results.lost().map(r -> r.result.state()).collect(Collectors.toList()))
        .containsExactly(StageExecutorState.LOST_ERROR);
  }
}
