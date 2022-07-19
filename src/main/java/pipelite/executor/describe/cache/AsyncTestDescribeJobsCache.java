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

import pipelite.configuration.ServiceConfiguration;
import pipelite.executor.AsyncTestExecutor;
import pipelite.executor.describe.DescribeJobsCache;
import pipelite.executor.describe.context.AsyncTestCacheContext;
import pipelite.executor.describe.context.AsyncTestExecutorContext;
import pipelite.executor.describe.context.AsyncTestRequestContext;
import pipelite.service.InternalErrorService;

public class AsyncTestDescribeJobsCache
    extends DescribeJobsCache<
        AsyncTestRequestContext,
        AsyncTestExecutorContext,
        AsyncTestCacheContext,
        AsyncTestExecutor> {

  private static final AsyncTestExecutorContext executorContext = new AsyncTestExecutorContext();
  private static final AsyncTestCacheContext cacheContext = new AsyncTestCacheContext();

  public AsyncTestDescribeJobsCache(
      ServiceConfiguration serviceConfiguration, InternalErrorService internalErrorService) {
    super(
        serviceConfiguration,
        internalErrorService,
        100,
        executor -> executorContext,
        executor -> cacheContext);
  }
}
