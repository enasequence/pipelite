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
package pipelite.executor.describe.cache;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import pipelite.configuration.ServiceConfiguration;
import pipelite.executor.AsyncTestExecutor;
import pipelite.executor.describe.context.cache.AsyncTestCacheContext;
import pipelite.executor.describe.context.executor.AsyncTestExecutorContext;
import pipelite.executor.describe.context.request.AsyncTestRequestContext;
import pipelite.executor.describe.poll.AsyncTestExecutorPollJobs;
import pipelite.service.InternalErrorService;

@Component
public class AsyncTestDescribeJobsCache
    extends DescribeJobsCache<
        AsyncTestRequestContext,
        AsyncTestExecutorContext,
        AsyncTestCacheContext,
        AsyncTestExecutor> {

  private final AsyncTestExecutorContext executorContext;
  private final AsyncTestCacheContext cacheContext = new AsyncTestCacheContext();

  public AsyncTestDescribeJobsCache(
      @Autowired ServiceConfiguration serviceConfiguration,
      @Autowired InternalErrorService internalErrorService,
      @Autowired AsyncTestExecutorPollJobs pollJobs) {
    super(serviceConfiguration, internalErrorService, 100);
    this.executorContext = new AsyncTestExecutorContext(pollJobs);
  }

  @Override
  public AsyncTestExecutorContext getExecutorContext(AsyncTestExecutor executor) {
    return executorContext;
  }

  @Override
  public AsyncTestCacheContext getCacheContext(AsyncTestExecutor executor) {
    return cacheContext;
  }
}
