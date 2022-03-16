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
package pipelite.executor;

import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import lombok.Getter;
import org.springframework.util.Assert;
import pipelite.executor.describe.DescribeJobs;
import pipelite.executor.describe.cache.TestDescribeJobsCache;
import pipelite.service.DescribeJobsCacheService;
import pipelite.stage.executor.StageExecutorRequest;
import pipelite.stage.executor.StageExecutorResult;
import pipelite.stage.executor.StageExecutorState;
import pipelite.stage.parameters.ExecutorParameters;
import pipelite.time.Time;

/** Asynchronous test executor. */
@Getter
public class AsyncTestExecutor
    extends AbstractAsyncExecutor<ExecutorParameters, TestDescribeJobsCache> {

  private static final AtomicInteger nextJobId = new AtomicInteger();
  private final StageExecutorState executorState;
  private final Function<StageExecutorRequest, StageExecutorResult> callback;
  private final Duration submitTime;
  private final Duration executionTime;

  public AsyncTestExecutor(
      StageExecutorState executorState, Duration submitTime, Duration executionTime) {
    Assert.notNull(executorState, "Missing executorState");
    this.executorState = executorState;
    this.callback = null;
    this.submitTime = submitTime;
    this.executionTime = executionTime;
  }

  public AsyncTestExecutor(Function<StageExecutorRequest, StageExecutorResult> callback) {
    Assert.notNull(callback, "Missing callback");
    this.executorState = null;
    this.callback = callback;
    this.submitTime = null;
    this.executionTime = null;
  }

  @Override
  protected TestDescribeJobsCache initDescribeJobsCache(
      DescribeJobsCacheService describeJobsCacheService) {
    return describeJobsCacheService.test();
  }

  protected DescribeJobs<
          TestDescribeJobsCache.RequestContext, TestDescribeJobsCache.ExecutorContext>
      describeJobs() {
    return getDescribeJobsCache().getDescribeJobs(this);
  }

  @Override
  protected SubmitResult submit(StageExecutorRequest request) {
    if (submitTime != null) {
      Time.wait(submitTime);
    }
    return new SubmitResult(
        String.valueOf(nextJobId.incrementAndGet()), StageExecutorResult.submitted());
  }

  @Override
  protected StageExecutorResult poll(StageExecutorRequest request) {
    return describeJobs()
        .getResult(describeJobsRequestContext(request), getExecutorParams().getPermanentErrors());
  }

  public static Map<TestDescribeJobsCache.RequestContext, StageExecutorResult> describeJobs(
      List<TestDescribeJobsCache.RequestContext> requests,
      TestDescribeJobsCache.ExecutorContext executorContext) {
    Map<TestDescribeJobsCache.RequestContext, StageExecutorResult> result = new HashMap<>();
    for (TestDescribeJobsCache.RequestContext request : requests) {
      if (request.getExecutionTime() != null
          && ZonedDateTime.now()
              .isBefore(request.getStartTime().plus(request.getExecutionTime()))) {
        continue;
      } else if (request.getCallback() != null) {
        result.put(request, request.getCallback().apply(request.getRequest()));
      } else {
        result.put(request, StageExecutorResult.from(request.getExecutorState()));
      }
    }
    return result;
  }

  private TestDescribeJobsCache.RequestContext describeJobsRequestContext(
      StageExecutorRequest request) {
    ZonedDateTime startTime = ZonedDateTime.now();
    return new TestDescribeJobsCache.RequestContext(
        getJobId(), request, startTime, executionTime, executorState, callback);
  }

  @Override
  public void terminate() {}
}
