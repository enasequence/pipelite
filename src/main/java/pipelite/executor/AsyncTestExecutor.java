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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import lombok.Getter;
import org.springframework.util.Assert;
import pipelite.executor.describe.DescribeJobs;
import pipelite.executor.describe.DescribeJobsPollRequests;
import pipelite.executor.describe.DescribeJobsResult;
import pipelite.executor.describe.DescribeJobsResults;
import pipelite.executor.describe.context.AsyncTestExecutorContext;
import pipelite.executor.describe.context.AsyncTestRequestContext;
import pipelite.service.PipeliteServices;
import pipelite.stage.executor.StageExecutorRequest;
import pipelite.stage.executor.StageExecutorResult;
import pipelite.stage.parameters.ExecutorParameters;
import pipelite.time.Time;

/** Asynchronous test executor. */
@Getter
public class AsyncTestExecutor
    extends AsyncExecutor<ExecutorParameters, AsyncTestRequestContext, AsyncTestExecutorContext> {

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
  protected AsyncTestRequestContext prepareRequestContext() {
    ZonedDateTime startTime = ZonedDateTime.now();
    return new AsyncTestRequestContext(
        getJobId(), getRequest(), startTime, executionTime, callback);
  }

  @Override
  protected DescribeJobs<AsyncTestRequestContext, AsyncTestExecutorContext> prepareDescribeJobs(
      PipeliteServices pipeliteServices) {
    return pipeliteServices.jobs().test().getDescribeJobs(this);
  }

  @Override
  protected SubmitJobResult submitJob() {
    if (submitTime != null) {
      Time.wait(submitTime);
    }
    String jobId = String.valueOf(nextJobId.incrementAndGet());
    return new SubmitJobResult(jobId);
  }

  @Override
  protected void terminateJob() {}

  public static DescribeJobsResults<AsyncTestRequestContext> pollJobs(
      DescribeJobsPollRequests<AsyncTestRequestContext> requests) {
    DescribeJobsResults<AsyncTestRequestContext> results = new DescribeJobsResults<>();
    for (AsyncTestRequestContext request : requests.requests.values()) {
      if (request.getExecutionTime() != null
          && ZonedDateTime.now()
              .isBefore(request.getStartTime().plus(request.getExecutionTime()))) {
        continue;
      }
      results.add(
          DescribeJobsResult.create(request, request.getCallback().apply(request.getRequest())));
    }
    return results;
  }
}
