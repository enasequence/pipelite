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
import lombok.experimental.Accessors;
import lombok.experimental.NonFinal;
import pipelite.executor.cmd.CmdRunner;
import pipelite.executor.describe.DescribeJobsPollRequests;
import pipelite.executor.describe.DescribeJobsResult;
import pipelite.executor.describe.DescribeJobsResults;
import pipelite.executor.describe.context.request.LsfRequestContext;
import pipelite.executor.describe.poll.LsfExecutorPollJobs;
import pipelite.executor.describe.recover.LsfExecutorRecoverJob;

@Value
@Accessors(fluent = true)
@NonFinal
@EqualsAndHashCode(callSuper = true)
public class LsfExecutorContext extends DefaultExecutorContext<LsfRequestContext> {

  private final CmdRunner cmdRunner;
  private final LsfExecutorPollJobs pollJobs;
  private final LsfExecutorRecoverJob recoverJob;

  public LsfExecutorContext(
      CmdRunner cmdRunner, LsfExecutorPollJobs pollJobs, LsfExecutorRecoverJob recoverJob) {
    super("LSF");
    this.cmdRunner = cmdRunner;
    this.pollJobs = pollJobs;
    this.recoverJob = recoverJob;
  }

  @Override
  public DescribeJobsResults<LsfRequestContext> pollJobs(
      DescribeJobsPollRequests<LsfRequestContext> requests) {
    return pollJobs.pollJobs(this, requests);
  }

  @Override
  public DescribeJobsResult<LsfRequestContext> recoverJob(LsfRequestContext request) {
    return recoverJob.recoverJob(this, request);
  }
}
