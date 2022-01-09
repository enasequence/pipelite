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

import lombok.EqualsAndHashCode;
import lombok.Value;
import lombok.extern.flogger.Flogger;
import pipelite.configuration.ServiceConfiguration;
import pipelite.executor.AbstractLsfExecutor;
import pipelite.executor.cmd.CmdRunner;
import pipelite.executor.describe.DescribeJobs;
import pipelite.service.InternalErrorService;
import pipelite.stage.parameters.SharedLsfExecutorParameters;

@Flogger
public class LsfDescribeJobsCache
    extends DescribeJobsCache<
        LsfDescribeJobsCache.RequestContext,
        LsfDescribeJobsCache.ExecutorContext,
        LsfDescribeJobsCache.CacheContext,
        AbstractLsfExecutor<SharedLsfExecutorParameters>> {

  @Value
  public static final class RequestContext {
    private final String jobId;
    @EqualsAndHashCode.Exclude private final String outFile;
  }

  @Value
  public static final class ExecutorContext {
    private final CmdRunner cmdRunner;
  }

  @Value
  public static final class CacheContext {
    private final String host;
  }

  public LsfDescribeJobsCache(
      ServiceConfiguration serviceConfiguration, InternalErrorService internalErrorService) {
    super(
        e ->
            new DescribeJobs<RequestContext, ExecutorContext>(
                serviceConfiguration,
                internalErrorService,
                100,
                executorContext(e),
                AbstractLsfExecutor::describeJobs),
        e -> cacheContext(e));
  }

  private static LsfDescribeJobsCache.ExecutorContext executorContext(
      AbstractLsfExecutor<SharedLsfExecutorParameters> executor) {
    return new ExecutorContext(CmdRunner.create(executor.getExecutorParams()));
  }

  private static LsfDescribeJobsCache.CacheContext cacheContext(
      AbstractLsfExecutor<SharedLsfExecutorParameters> executor) {
    return new CacheContext(executor.getExecutorParams().getHost());
  }
}
