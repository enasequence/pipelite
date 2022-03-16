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
package pipelite.executor.describe.cache;

import io.fabric8.kubernetes.client.KubernetesClient;
import lombok.Value;
import lombok.extern.flogger.Flogger;
import pipelite.configuration.ServiceConfiguration;
import pipelite.executor.KubernetesExecutor;
import pipelite.executor.describe.DescribeJobs;
import pipelite.service.InternalErrorService;
import pipelite.stage.parameters.KubernetesExecutorParameters;

@Flogger
public class KubernetesDescribeJobsCache
    extends DescribeJobsCache<
        String, // RequestContext: jobId
        KubernetesDescribeJobsCache.ExecutorContext,
        KubernetesDescribeJobsCache.CacheContext,
        KubernetesExecutor> {

  @Value
  public static final class ExecutorContext {
    private final KubernetesClient kubernetesClient;
    private final String namespace;
  }

  @Value
  public static final class CacheContext {
    private final String context;
    private final String namespace;
  }

  public KubernetesDescribeJobsCache(
      ServiceConfiguration serviceConfiguration, InternalErrorService internalErrorService) {
    super(
        e ->
            new DescribeJobs<>(
                serviceConfiguration,
                internalErrorService,
                null, // Request all at once
                executorContext(e),
                KubernetesExecutor::describeJobs),
        e -> cacheContext(e));
  }

  private static ExecutorContext executorContext(KubernetesExecutor executor) {
    KubernetesExecutorParameters params = executor.getExecutorParams();
    return new ExecutorContext(
        KubernetesExecutor.kubernetesClient(params.getContext()), params.getNamespace());
  }

  private static CacheContext cacheContext(KubernetesExecutor executor) {
    KubernetesExecutorParameters params = executor.getExecutorParams();
    return new CacheContext(params.getContext(), params.getNamespace());
  }
}
