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
package pipelite.executor.describe;

import org.junit.jupiter.api.Test;
import pipelite.exception.PipeliteException;
import pipelite.executor.describe.context.DefaultRequestContext;
import pipelite.stage.executor.StageExecutorResult;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

public class DescribeJobsResultTest {

  private final StageExecutorResult result = StageExecutorResult.success();

  private final String validJobId = "validJobId";
  private final String invalidJobId = "invalidJobId";

  private final DefaultRequestContext request = new DefaultRequestContext(validJobId);
  private final DescribeJobsPollRequests<DefaultRequestContext> requests =
      new DescribeJobsPollRequests<>(Arrays.asList(request));

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
    assertThatThrownBy(() -> DescribeJobsResult.create(requests, invalidJobId, null))
        .isInstanceOf(PipeliteException.class);
    assertThatThrownBy(() -> DescribeJobsResult.create(requests, invalidJobId, result))
        .isInstanceOf(PipeliteException.class);
  }
}
