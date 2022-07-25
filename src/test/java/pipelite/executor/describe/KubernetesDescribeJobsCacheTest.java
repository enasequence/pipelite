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
import pipelite.executor.KubernetesExecutor;
import pipelite.executor.describe.cache.KubernetesDescribeJobsCache;
import pipelite.executor.describe.context.DefaultRequestContext;
import pipelite.executor.describe.context.KubernetesExecutorContext;
import pipelite.service.InternalErrorService;
import pipelite.stage.parameters.KubernetesExecutorParameters;

public class KubernetesDescribeJobsCacheTest {

  @Test
  public void test() {
    KubernetesDescribeJobsCache cache =
        new KubernetesDescribeJobsCache(
            Mockito.mock(ServiceConfiguration.class), Mockito.mock(InternalErrorService.class));

    KubernetesExecutorParameters namespace1 =
        KubernetesExecutorParameters.builder().namespace("1").build();
    KubernetesExecutorParameters namespace2 =
        KubernetesExecutorParameters.builder().namespace("2").build();

    KubernetesExecutor executor1Namespace1 = new KubernetesExecutor();
    KubernetesExecutor executor2Namespace1 = new KubernetesExecutor();
    KubernetesExecutor executor3Namespace2 = new KubernetesExecutor();
    KubernetesExecutor executor4Namespace1 = new KubernetesExecutor();

    executor1Namespace1.setExecutorParams(namespace1);
    executor2Namespace1.setExecutorParams(namespace1);
    executor3Namespace2.setExecutorParams(namespace2);
    executor4Namespace1.setExecutorParams(namespace1);

    assertThat(cache.getCacheContext(executor1Namespace1).getNamespace())
        .isEqualTo(namespace1.getNamespace());
    assertThat(cache.getCacheContext(executor2Namespace1).getNamespace())
        .isEqualTo(namespace1.getNamespace());
    assertThat(cache.getCacheContext(executor3Namespace2).getNamespace())
        .isEqualTo(namespace2.getNamespace());
    assertThat(cache.getCacheContext(executor4Namespace1).getNamespace())
        .isEqualTo(namespace1.getNamespace());

    DescribeJobs<DefaultRequestContext, KubernetesExecutorContext> describeJobs1 =
        cache.getDescribeJobs(executor1Namespace1);
    DescribeJobs<DefaultRequestContext, KubernetesExecutorContext> describeJobs2 =
        cache.getDescribeJobs(executor2Namespace1);
    DescribeJobs<DefaultRequestContext, KubernetesExecutorContext> describeJobs3 =
        cache.getDescribeJobs(executor3Namespace2);
    DescribeJobs<DefaultRequestContext, KubernetesExecutorContext> describeJobs4 =
        cache.getDescribeJobs(executor4Namespace1);

    assertThat(describeJobs1 == describeJobs2).isTrue();
    assertThat(describeJobs1 != describeJobs3).isTrue();
    assertThat(describeJobs1 == describeJobs4).isTrue();
  }
}
