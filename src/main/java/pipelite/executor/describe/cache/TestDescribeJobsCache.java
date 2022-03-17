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

import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.function.Function;
import lombok.EqualsAndHashCode;
import lombok.Value;
import lombok.extern.flogger.Flogger;
import pipelite.configuration.ServiceConfiguration;
import pipelite.executor.AsyncTestExecutor;
import pipelite.executor.describe.DescribeJobs;
import pipelite.service.InternalErrorService;
import pipelite.stage.executor.StageExecutorRequest;
import pipelite.stage.executor.StageExecutorResult;

@Flogger
public class TestDescribeJobsCache
    extends DescribeJobsCache<
        TestDescribeJobsCache.RequestContext,
        TestDescribeJobsCache.ExecutorContext,
        TestDescribeJobsCache.CacheContext,
        AsyncTestExecutor> {

  @Value
  public static final class RequestContext {
    private final String jobId;
    @EqualsAndHashCode.Exclude private final StageExecutorRequest request;
    @EqualsAndHashCode.Exclude private final ZonedDateTime startTime;
    @EqualsAndHashCode.Exclude private final Duration executionTime;

    @EqualsAndHashCode.Exclude
    private final Function<StageExecutorRequest, StageExecutorResult> callback;
  }

  @Value
  public static final class ExecutorContext {}

  @Value
  public static final class CacheContext {}

  private static final ExecutorContext executorContext = new ExecutorContext();
  private static final CacheContext cacheContext = new CacheContext();

  public TestDescribeJobsCache(
      ServiceConfiguration serviceConfiguration, InternalErrorService internalErrorService) {
    super(
        e ->
            new DescribeJobs<>(
                serviceConfiguration,
                internalErrorService,
                100,
                executorContext(),
                AsyncTestExecutor::describeJobs),
        e -> cacheContext());
  }

  private static TestDescribeJobsCache.ExecutorContext executorContext() {
    return executorContext;
  }

  private static TestDescribeJobsCache.CacheContext cacheContext() {
    return cacheContext;
  }
}
