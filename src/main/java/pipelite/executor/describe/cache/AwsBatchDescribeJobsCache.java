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

import com.amazonaws.services.batch.AWSBatch;
import com.amazonaws.services.batch.AWSBatchClientBuilder;
import lombok.Value;
import lombok.extern.flogger.Flogger;
import pipelite.configuration.ServiceConfiguration;
import pipelite.executor.AwsBatchExecutor;
import pipelite.executor.describe.DescribeJobs;
import pipelite.service.InternalErrorService;
import pipelite.stage.parameters.AwsBatchExecutorParameters;

@Flogger
public class AwsBatchDescribeJobsCache
    extends DescribeJobsCache<
        String, // RequestContext: jobId
        AwsBatchDescribeJobsCache.ExecutorContext,
        AwsBatchDescribeJobsCache.CacheContext,
        AwsBatchExecutor> {

  @Value
  public static final class ExecutorContext {
    private final AWSBatch awsBatch;
  }

  @Value
  public static final class CacheContext {
    private final String region;
  }

  public AwsBatchDescribeJobsCache(
      ServiceConfiguration serviceConfiguration, InternalErrorService internalErrorService) {
    super(
        e -> {
          AWSBatchClientBuilder awsBuilder = AWSBatchClientBuilder.standard();
          String region = e.getExecutorParams().getRegion();
          if (region != null) {
            awsBuilder.setRegion(region);
          }
          return new DescribeJobs<>(
              serviceConfiguration,
              internalErrorService,
              100,
              executorContext(e),
              AwsBatchExecutor::describeJobs);
        },
        e -> cacheContext(e));
  }

  private static AwsBatchDescribeJobsCache.ExecutorContext executorContext(
      AwsBatchExecutor executor) {
    AwsBatchExecutorParameters params = executor.getExecutorParams();
    return new ExecutorContext(AwsBatchExecutor.awsBatchClient(params.getRegion()));
  }

  private static AwsBatchDescribeJobsCache.CacheContext cacheContext(AwsBatchExecutor executor) {
    AwsBatchExecutorParameters params = executor.getExecutorParams();
    return new CacheContext(params.getRegion());
  }
}
