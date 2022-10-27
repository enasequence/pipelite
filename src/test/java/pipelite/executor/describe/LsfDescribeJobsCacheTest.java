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

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import pipelite.configuration.ServiceConfiguration;
import pipelite.executor.AbstractLsfExecutor;
import pipelite.executor.SimpleLsfExecutor;
import pipelite.executor.describe.cache.LsfDescribeJobsCache;
import pipelite.executor.describe.context.executor.LsfExecutorContext;
import pipelite.executor.describe.context.request.LsfRequestContext;
import pipelite.executor.describe.poll.LsfExecutorPollJobs;
import pipelite.executor.describe.recover.LsfExecutorRecoverJob;
import pipelite.service.InternalErrorService;
import pipelite.stage.parameters.SimpleLsfExecutorParameters;

public class LsfDescribeJobsCacheTest {

  @Test
  public void test() {
    LsfDescribeJobsCache cache =
        new LsfDescribeJobsCache(
            Mockito.mock(ServiceConfiguration.class),
            Mockito.mock(InternalErrorService.class),
            new LsfExecutorPollJobs(),
            new LsfExecutorRecoverJob());

    SimpleLsfExecutorParameters host1 = SimpleLsfExecutorParameters.builder().host("1").build();
    SimpleLsfExecutorParameters host2 = SimpleLsfExecutorParameters.builder().host("2").build();

    AbstractLsfExecutor executor1Host1 = new SimpleLsfExecutor();
    AbstractLsfExecutor executor2Host1 = new SimpleLsfExecutor();
    AbstractLsfExecutor executor3Host2 = new SimpleLsfExecutor();
    AbstractLsfExecutor executor4Host1 = new SimpleLsfExecutor();

    executor1Host1.setExecutorParams(host1);
    executor2Host1.setExecutorParams(host1);
    executor3Host2.setExecutorParams(host2);
    executor4Host1.setExecutorParams(host1);

    assertThat(cache.getCacheContext(executor1Host1).getHost()).isEqualTo(host1.getHost());
    assertThat(cache.getCacheContext(executor2Host1).getHost()).isEqualTo(host1.getHost());
    assertThat(cache.getCacheContext(executor3Host2).getHost()).isEqualTo(host2.getHost());
    assertThat(cache.getCacheContext(executor4Host1).getHost()).isEqualTo(host1.getHost());

    DescribeJobs<LsfRequestContext, LsfExecutorContext> describeJobs1 =
        cache.getDescribeJobs(executor1Host1);
    DescribeJobs<LsfRequestContext, LsfExecutorContext> describeJobs2 =
        cache.getDescribeJobs(executor2Host1);
    DescribeJobs<LsfRequestContext, LsfExecutorContext> describeJobs3 =
        cache.getDescribeJobs(executor3Host2);
    DescribeJobs<LsfRequestContext, LsfExecutorContext> describeJobs4 =
        cache.getDescribeJobs(executor4Host1);

    assertThat(describeJobs1 == describeJobs2).isTrue();
    assertThat(describeJobs1 != describeJobs3).isTrue();
    assertThat(describeJobs1 == describeJobs4).isTrue();
  }
}
