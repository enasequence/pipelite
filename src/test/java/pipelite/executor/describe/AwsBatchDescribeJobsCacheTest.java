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

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import pipelite.configuration.ServiceConfiguration;
import pipelite.executor.AwsBatchExecutor;
import pipelite.executor.describe.cache.AwsBatchDescribeJobsCache;
import pipelite.executor.describe.context.AwsBatchExecutorContext;
import pipelite.executor.describe.context.DefaultRequestContext;
import pipelite.service.InternalErrorService;
import pipelite.stage.parameters.AwsBatchExecutorParameters;

@Tag("aws-test")
public class AwsBatchDescribeJobsCacheTest {

  @Test
  public void test() {
    AwsBatchDescribeJobsCache cache =
        new AwsBatchDescribeJobsCache(
            Mockito.mock(ServiceConfiguration.class), Mockito.mock(InternalErrorService.class));

    AwsBatchExecutorParameters region1 = AwsBatchExecutorParameters.builder().region("1").build();
    AwsBatchExecutorParameters region2 = AwsBatchExecutorParameters.builder().region("2").build();

    AwsBatchExecutor executor1Region1 = new AwsBatchExecutor();
    AwsBatchExecutor executor2Region1 = new AwsBatchExecutor();
    AwsBatchExecutor executor3Region2 = new AwsBatchExecutor();
    AwsBatchExecutor executor4Region1 = new AwsBatchExecutor();

    executor1Region1.setExecutorParams(region1);
    executor2Region1.setExecutorParams(region1);
    executor3Region2.setExecutorParams(region2);
    executor4Region1.setExecutorParams(region1);

    assertThat(cache.getCacheContext(executor1Region1).getRegion()).isEqualTo(region1.getRegion());
    assertThat(cache.getCacheContext(executor2Region1).getRegion()).isEqualTo(region1.getRegion());
    assertThat(cache.getCacheContext(executor3Region2).getRegion()).isEqualTo(region2.getRegion());
    assertThat(cache.getCacheContext(executor4Region1).getRegion()).isEqualTo(region1.getRegion());

    DescribeJobs<DefaultRequestContext, AwsBatchExecutorContext> describeJobs1 =
        cache.getDescribeJobs(executor1Region1);
    DescribeJobs<DefaultRequestContext, AwsBatchExecutorContext> describeJobs2 =
        cache.getDescribeJobs(executor2Region1);
    DescribeJobs<DefaultRequestContext, AwsBatchExecutorContext> describeJobs3 =
        cache.getDescribeJobs(executor3Region2);
    DescribeJobs<DefaultRequestContext, AwsBatchExecutorContext> describeJobs4 =
        cache.getDescribeJobs(executor4Region1);

    assertThat(describeJobs1 == describeJobs2).isTrue();
    assertThat(describeJobs1 != describeJobs3).isTrue();
    assertThat(describeJobs1 == describeJobs4).isTrue();
  }
}
