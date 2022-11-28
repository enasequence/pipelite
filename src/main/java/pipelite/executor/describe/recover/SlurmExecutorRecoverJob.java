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
package pipelite.executor.describe.recover;

import lombok.extern.flogger.Flogger;
import org.springframework.stereotype.Component;
import pipelite.exception.PipeliteException;
import pipelite.executor.describe.DescribeJobsResult;
import pipelite.executor.describe.context.executor.SlurmExecutorContext;
import pipelite.executor.describe.context.request.SlurmRequestContext;
import pipelite.executor.describe.poll.SlurmExecutorPollJobs;

@Component
@Flogger
public class SlurmExecutorRecoverJob
    implements RecoverJob<SlurmExecutorContext, SlurmRequestContext> {

  @Override
  public DescribeJobsResult<SlurmRequestContext> recoverJob(
      SlurmExecutorContext executorContext, SlurmRequestContext request) {
    DescribeJobsResult.Builder resultBuilder = DescribeJobsResult.builder(request);
    DescribeJobsResult<SlurmRequestContext> result =
        SlurmExecutorPollJobs.extractSacctJobResult(executorContext, resultBuilder);

    if (!result.result.isCompleted()) {
      throw new PipeliteException(
          "Unexpected SLURM recover job result: " + result.result.state().name());
    }
    return result;
  }
}
