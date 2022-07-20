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
package pipelite.executor.describe.context;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.Value;
import lombok.experimental.NonFinal;
import pipelite.executor.describe.DescribeJobsPollRequests;
import pipelite.executor.describe.DescribeJobsResult;
import pipelite.executor.describe.DescribeJobsResults;
import pipelite.stage.executor.StageExecutorResult;

@Value
@NonFinal
public class DefaultExecutorContext<RequestContext extends DefaultRequestContext> {

  @JsonIgnore private final String executorName;
  @JsonIgnore private final PollJobsCallback<RequestContext> pollJobsCallback;
  @JsonIgnore private final RecoverJobCallback<RequestContext> recoverJobCallback;

  @FunctionalInterface
  public interface PollJobsCallback<RequestContext extends DefaultRequestContext> {
    DescribeJobsResults<RequestContext> pollJobs(DescribeJobsPollRequests<RequestContext> requests);
  }

  @FunctionalInterface
  public interface RecoverJobCallback<RequestContext extends DefaultRequestContext> {
    DescribeJobsResult<RequestContext> recoverJob(RequestContext request);
  }

  public DefaultExecutorContext(
      String executorName,
      PollJobsCallback<RequestContext> pollJobsCallback,
      RecoverJobCallback<RequestContext> recoverJobCallback) {
    this.executorName = executorName;
    this.pollJobsCallback = pollJobsCallback;
    this.recoverJobCallback = recoverJobCallback;
  }

  public String executorName() {
    return executorName;
  }

  public DescribeJobsResults<RequestContext> pollJobs(
      DescribeJobsPollRequests<RequestContext> requests) {
    return pollJobsCallback.pollJobs(requests);
  }

  public DescribeJobsResult<RequestContext> recoverJob(RequestContext request) {
    if (recoverJobCallback != null) {
      return recoverJobCallback.recoverJob(request);
    }
    return DescribeJobsResult.create(request, StageExecutorResult.error());
  }
}
