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
package pipelite.executor.describe.poll;

import java.time.ZonedDateTime;
import lombok.extern.flogger.Flogger;
import org.springframework.stereotype.Component;
import pipelite.executor.describe.DescribeJobsPollRequests;
import pipelite.executor.describe.DescribeJobsResult;
import pipelite.executor.describe.DescribeJobsResults;
import pipelite.executor.describe.context.executor.AsyncTestExecutorContext;
import pipelite.executor.describe.context.request.AsyncTestRequestContext;

@Component
@Flogger
public class AsyncTestExecutorPollJobs
    implements PollJobs<AsyncTestExecutorContext, AsyncTestRequestContext> {

  @Override
  public DescribeJobsResults<AsyncTestRequestContext> pollJobs(
      AsyncTestExecutorContext executorContext,
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
