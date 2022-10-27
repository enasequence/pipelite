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
package pipelite.executor.describe.context.executor;

import lombok.EqualsAndHashCode;
import lombok.Value;
import lombok.experimental.NonFinal;
import pipelite.executor.describe.DescribeJobsPollRequests;
import pipelite.executor.describe.DescribeJobsResults;
import pipelite.executor.describe.context.request.AsyncTestRequestContext;
import pipelite.executor.describe.poll.AsyncTestExecutorPollJobs;

@Value
@NonFinal
@EqualsAndHashCode(callSuper = true)
public class AsyncTestExecutorContext extends DefaultExecutorContext<AsyncTestRequestContext> {

  private final AsyncTestExecutorPollJobs pollJobs;

  public AsyncTestExecutorContext(AsyncTestExecutorPollJobs pollJobs) {
    super("AsyncTest");
    this.pollJobs = pollJobs;
  }

  @Override
  public DescribeJobsResults<AsyncTestRequestContext> pollJobs(
      DescribeJobsPollRequests<AsyncTestRequestContext> requests) {
    return pollJobs.pollJobs(this, requests);
  }
}
