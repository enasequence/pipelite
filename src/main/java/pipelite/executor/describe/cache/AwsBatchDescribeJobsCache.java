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
import pipelite.executor.AwsBatchExecutor;
import pipelite.executor.describe.DescribeJobsCache;
import pipelite.executor.describe.context.AwsBatchCacheContext;
import pipelite.executor.describe.context.AwsBatchExecutorContext;
import pipelite.executor.describe.context.DefaultRequestContext;
import pipelite.service.InternalErrorService;
import pipelite.stage.parameters.AwsBatchExecutorParameters;

public class AwsBatchDescribeJobsCache
    extends DescribeJobsCache<
        DefaultRequestContext, AwsBatchExecutorContext, AwsBatchCacheContext, AwsBatchExecutor> {

  public AwsBatchDescribeJobsCache(
      ServiceConfiguration serviceConfiguration, InternalErrorService internalErrorService) {
    super(
        serviceConfiguration,
        internalErrorService,
        100,
        executor -> executorContext(executor),
        executor -> cacheContext(executor));
  }

  private static AwsBatchExecutorContext executorContext(AwsBatchExecutor executor) {
    AwsBatchExecutorParameters params = executor.getExecutorParams();
    return new AwsBatchExecutorContext(AwsBatchExecutor.client(params.getRegion()));
  }

  private static AwsBatchCacheContext cacheContext(AwsBatchExecutor executor) {
    AwsBatchExecutorParameters params = executor.getExecutorParams();
    return new AwsBatchCacheContext(params.getRegion());
  }
}
