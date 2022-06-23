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
import pipelite.executor.async.PollResult;
import pipelite.executor.async.SubmitResult;
import pipelite.executor.describe.DescribeJobs;
import pipelite.executor.describe.cache.TestDescribeJobsCache;
import pipelite.service.DescribeJobsCacheService;
import pipelite.stage.executor.StageExecutorRequest;
import pipelite.stage.executor.StageExecutorResult;
import pipelite.stage.parameters.ExecutorParameters;
import pipelite.time.Time;

/** Asynchronous test executor. */
@Getter
public class AsyncTestExecutor extends AsyncExecutor<ExecutorParameters, TestDescribeJobsCache> {

  private static final AtomicInteger nextJobId = new AtomicInteger();
  private final Function<StageExecutorRequest, StageExecutorResult> callback;
  private final Duration submitTime;
  private final Duration executionTime;

  public AsyncTestExecutor(
      Function<StageExecutorRequest, StageExecutorResult> callback,
      Duration submitTime,
      Duration executionTime) {
    Assert.notNull(callback, "Missing callback");
    this.callback = callback;
    this.submitTime = submitTime;
    this.executionTime = executionTime;
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
  protected SubmitResult submit() {
    if (submitTime != null) {
      Time.wait(submitTime);
    }
    return SubmitResult.valueOf(String.valueOf(nextJobId.incrementAndGet()));
  }

  @Override
  protected PollResult poll() {
    return PollResult.valueOf(
        describeJobs()
            .getResult(
                describeJobsRequestContext(getRequest()),
                getExecutorParams().getPermanentErrors()));
  }

  @Override
  protected boolean afterPoll(PollResult pollResult) {
    return true;
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
      }
      result.put(request, request.getCallback().apply(request.getRequest()));
    }
    return result;
  }

  private TestDescribeJobsCache.RequestContext describeJobsRequestContext(
      StageExecutorRequest request) {
    ZonedDateTime startTime = ZonedDateTime.now();
    return new TestDescribeJobsCache.RequestContext(
        getJobId(), request, startTime, executionTime, callback);
  }

  @Override
  public void terminate() {}
}
