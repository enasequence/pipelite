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
import pipelite.executor.AbstractSlurmExecutor;
import pipelite.executor.SimpleSlurmExecutor;
import pipelite.executor.describe.cache.SlurmDescribeJobsCache;
import pipelite.executor.describe.context.executor.SlurmExecutorContext;
import pipelite.executor.describe.context.request.SlurmRequestContext;
import pipelite.executor.describe.poll.SlurmExecutorPollJobs;
import pipelite.executor.describe.recover.SlurmExecutorRecoverJob;
import pipelite.service.InternalErrorService;
import pipelite.stage.parameters.SimpleSlurmExecutorParameters;

public class SlurmDescribeJobsCacheTest {

  @Test
  public void test() {
    SlurmDescribeJobsCache cache =
        new SlurmDescribeJobsCache(
            Mockito.mock(ServiceConfiguration.class),
            Mockito.mock(InternalErrorService.class),
            new SlurmExecutorPollJobs(),
            new SlurmExecutorRecoverJob());

    SimpleSlurmExecutorParameters host1 = SimpleSlurmExecutorParameters.builder().host("1").build();
    SimpleSlurmExecutorParameters host2 = SimpleSlurmExecutorParameters.builder().host("2").build();

    AbstractSlurmExecutor executor1Host1 = new SimpleSlurmExecutor();
    AbstractSlurmExecutor executor2Host1 = new SimpleSlurmExecutor();
    AbstractSlurmExecutor executor3Host2 = new SimpleSlurmExecutor();
    AbstractSlurmExecutor executor4Host1 = new SimpleSlurmExecutor();

    executor1Host1.setExecutorParams(host1);
    executor2Host1.setExecutorParams(host1);
    executor3Host2.setExecutorParams(host2);
    executor4Host1.setExecutorParams(host1);

    assertThat(cache.getCacheContext(executor1Host1).getHost()).isEqualTo(host1.getHost());
    assertThat(cache.getCacheContext(executor2Host1).getHost()).isEqualTo(host1.getHost());
    assertThat(cache.getCacheContext(executor3Host2).getHost()).isEqualTo(host2.getHost());
    assertThat(cache.getCacheContext(executor4Host1).getHost()).isEqualTo(host1.getHost());

    DescribeJobs<SlurmRequestContext, SlurmExecutorContext> describeJobs1 =
        cache.getDescribeJobs(executor1Host1);
    DescribeJobs<SlurmRequestContext, SlurmExecutorContext> describeJobs2 =
        cache.getDescribeJobs(executor2Host1);
    DescribeJobs<SlurmRequestContext, SlurmExecutorContext> describeJobs3 =
        cache.getDescribeJobs(executor3Host2);
    DescribeJobs<SlurmRequestContext, SlurmExecutorContext> describeJobs4 =
        cache.getDescribeJobs(executor4Host1);

    assertThat(describeJobs1 == describeJobs2).isTrue();
    assertThat(describeJobs1 != describeJobs3).isTrue();
    assertThat(describeJobs1 == describeJobs4).isTrue();
  }
}
