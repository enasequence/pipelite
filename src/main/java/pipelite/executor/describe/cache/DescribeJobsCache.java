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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.PreDestroy;
import pipelite.configuration.ServiceConfiguration;
import pipelite.executor.describe.DescribeJobs;
import pipelite.executor.describe.context.DefaultCacheContext;
import pipelite.executor.describe.context.DefaultExecutorContext;
import pipelite.executor.describe.context.DefaultRequestContext;
import pipelite.service.InternalErrorService;
import pipelite.stage.executor.StageExecutor;

public abstract class DescribeJobsCache<
    RequestContext extends DefaultRequestContext,
    ExecutorContext extends DefaultExecutorContext<RequestContext>,
    CacheContext extends DefaultCacheContext,
    Executor extends StageExecutor> {

  private final ServiceConfiguration serviceConfiguration;
  private final InternalErrorService internalErrorService;
  private final Integer requestLimit;

  private final Map<CacheContext, DescribeJobs<RequestContext, ExecutorContext>> cache =
      new ConcurrentHashMap<>();

  public DescribeJobsCache(
      ServiceConfiguration serviceConfiguration,
      InternalErrorService internalErrorService,
      Integer requestLimit) {
    this.serviceConfiguration = serviceConfiguration;
    this.internalErrorService = internalErrorService;
    this.requestLimit = requestLimit;
  }

  public abstract ExecutorContext getExecutorContext(Executor executor);

  public abstract CacheContext getCacheContext(Executor executor);

  public DescribeJobs<RequestContext, ExecutorContext> getDescribeJobs(Executor executor) {
    return cache.computeIfAbsent(getCacheContext(executor), k -> createDescribeJobs(executor));
  }

  public DescribeJobs<RequestContext, ExecutorContext> createDescribeJobs(Executor executor) {
    return new DescribeJobs<>(
        serviceConfiguration, internalErrorService, requestLimit, getExecutorContext(executor));
  }

  public Integer requestLimit() {
    return requestLimit;
  }

  @PreDestroy
  public void shutdown() {
    cache.values().forEach(describeJobs -> describeJobs.shutdown());
  }
}
