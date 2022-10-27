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
import pipelite.executor.AwsBatchExecutor;
import pipelite.executor.describe.context.cache.AwsBatchCacheContext;
import pipelite.executor.describe.context.executor.AwsBatchExecutorContext;
import pipelite.executor.describe.context.request.DefaultRequestContext;
import pipelite.executor.describe.poll.AwsBatchExecutorPollJobs;
import pipelite.service.InternalErrorService;
import pipelite.stage.parameters.AwsBatchExecutorParameters;

@Component
public class AwsBatchDescribeJobsCache
    extends DescribeJobsCache<
        DefaultRequestContext, AwsBatchExecutorContext, AwsBatchCacheContext, AwsBatchExecutor> {

  private final AwsBatchExecutorPollJobs pollJobs;

  public AwsBatchDescribeJobsCache(
      @Autowired ServiceConfiguration serviceConfiguration,
      @Autowired InternalErrorService internalErrorService,
      @Autowired AwsBatchExecutorPollJobs pollJobs) {
    super(serviceConfiguration, internalErrorService, 100);
    this.pollJobs = pollJobs;
  }

  @Override
  public AwsBatchExecutorContext getExecutorContext(AwsBatchExecutor executor) {
    AwsBatchExecutorParameters params = executor.getExecutorParams();
    return new AwsBatchExecutorContext(AwsBatchExecutor.client(params.getRegion()), pollJobs);
  }

  @Override
  public AwsBatchCacheContext getCacheContext(AwsBatchExecutor executor) {
    AwsBatchExecutorParameters params = executor.getExecutorParams();
    return new AwsBatchCacheContext(params.getRegion());
  }
}
