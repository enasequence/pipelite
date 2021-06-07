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
package pipelite.executor.context;

import com.amazonaws.services.batch.AWSBatch;
import com.amazonaws.services.batch.AWSBatchClientBuilder;
import lombok.Value;
import lombok.extern.flogger.Flogger;
import pipelite.configuration.ServiceConfiguration;
import pipelite.executor.AwsBatchExecutor;
import pipelite.executor.task.RetryTaskAggregator;
import pipelite.service.InternalErrorService;

/** Context for tasks that can be shared between AWSBatch executors. */
@Flogger
public class AwsBatchContextCache
    extends SharedContextCache<
        AwsBatchExecutor, AwsBatchContextCache.ContextId, AwsBatchContextCache.Context> {

  @Value
  public static final class ContextId {
    private final String region;
  }

  public static final class Context extends SharedContextCache.Context<AWSBatch> {
    public final RetryTaskAggregator<String, AWSBatch> describeJobs;

    public Context(
        AWSBatch awsBatch,
        ServiceConfiguration serviceConfiguration,
        InternalErrorService internalErrorServices) {
      super(awsBatch);
      describeJobs =
          new RetryTaskAggregator<>(
              serviceConfiguration,
              internalErrorServices,
              100,
              awsBatch,
              AwsBatchExecutor::describeJobs);
    }
  }

  public AwsBatchContextCache(
      ServiceConfiguration serviceConfiguration, InternalErrorService internalErrorService) {
    super(
        e -> {
          AWSBatchClientBuilder awsBuilder = AWSBatchClientBuilder.standard();
          String region = e.getExecutorParams().getRegion();
          if (region != null) {
            awsBuilder.setRegion(region);
          }
          return new AwsBatchContextCache.Context(
              awsBuilder.build(), serviceConfiguration, internalErrorService);
        },
        e -> new AwsBatchContextCache.ContextId(e.getExecutorParams().getRegion()));
    registerMakeRequests(
        () -> getContexts().forEach(context -> context.describeJobs.makeRequests()));
  }
}
