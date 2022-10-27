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

import com.amazonaws.services.batch.model.DescribeJobsRequest;
import com.amazonaws.services.batch.model.JobDetail;
import lombok.extern.flogger.Flogger;
import org.springframework.stereotype.Component;
import pipelite.executor.describe.DescribeJobsPollRequests;
import pipelite.executor.describe.DescribeJobsResult;
import pipelite.executor.describe.DescribeJobsResults;
import pipelite.executor.describe.context.executor.AwsBatchExecutorContext;
import pipelite.executor.describe.context.request.DefaultRequestContext;
import pipelite.retryable.RetryableExternalAction;
import pipelite.stage.executor.StageExecutorResult;

@Component
@Flogger
public class AwsBatchExecutorPollJobs
    implements PollJobs<AwsBatchExecutorContext, DefaultRequestContext> {

  @Override
  public DescribeJobsResults<DefaultRequestContext> pollJobs(
      AwsBatchExecutorContext executorContext,
      DescribeJobsPollRequests<DefaultRequestContext> requests) {
    DescribeJobsResults<DefaultRequestContext> results = new DescribeJobsResults<>();
    com.amazonaws.services.batch.model.DescribeJobsResult jobResult =
        RetryableExternalAction.execute(
            () ->
                executorContext
                    .awsBatch()
                    .describeJobs(new DescribeJobsRequest().withJobs(requests.jobIds)));
    jobResult
        .getJobs()
        .forEach(
            j ->
                results.add(
                    DescribeJobsResult.create(requests, j.getJobId(), extractJobResult(j))));
    return results;
  }

  // TOOO: exit codes
  protected static StageExecutorResult extractJobResult(JobDetail jobDetail) {
    switch (jobDetail.getStatus()) {
      case "SUCCEEDED":
        return StageExecutorResult.success();
      case "FAILED":
        return StageExecutorResult.executionError();
    }
    return StageExecutorResult.active();
  }
}
