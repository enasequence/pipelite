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
package pipelite.executor;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.flogger.Flogger;
import pipelite.executor.cmd.CmdRunner;
import pipelite.executor.describe.DescribeJobs;
import pipelite.executor.describe.DescribeJobsPollRequests;
import pipelite.executor.describe.DescribeJobsResult;
import pipelite.executor.describe.DescribeJobsResults;
import pipelite.executor.describe.context.SlurmExecutorContext;
import pipelite.executor.describe.context.SlurmRequestContext;
import pipelite.service.PipeliteServices;
import pipelite.stage.executor.StageExecutorRequest;
import pipelite.stage.parameters.AbstractSlurmExecutorParameters;
import pipelite.stage.path.SlurmLogFilePathResolver;

/** Executes a command using LSF. */
@Flogger
@Getter
@Setter
@JsonIgnoreProperties({"cmdRunner"})
public abstract class AbstractSlurmExecutor<T extends AbstractSlurmExecutorParameters>
    extends AsyncCmdExecutor<T, SlurmRequestContext, SlurmExecutorContext>
    implements JsonSerializableExecutor {

  public AbstractSlurmExecutor() {
    super("SLURM", new SlurmLogFilePathResolver());
  }

  @Override
  protected SlurmRequestContext prepareRequestContext() {
    return new SlurmRequestContext(getJobId(), outFile);
  }

  @Override
  protected DescribeJobs<SlurmRequestContext, SlurmExecutorContext> prepareDescribeJobs(
      PipeliteServices pipeliteServices) {
    return pipeliteServices
        .jobs()
        .slurm()
        .getDescribeJobs((AbstractSlurmExecutor<AbstractSlurmExecutorParameters>) this);
  }

  /**
   * Returns the submit command.
   *
   * @return the submit command.
   */
  public abstract String getSubmitCmd(StageExecutorRequest request);

  /** Polls job execution results. */
  public static DescribeJobsResults<SlurmRequestContext> pollJobs(
      CmdRunner cmdRunner, DescribeJobsPollRequests<SlurmRequestContext> requests) {
    // TODO
    return null;
  }

  /** Recovers job execution result. */
  public static DescribeJobsResult<SlurmRequestContext> recoverJob(
      CmdRunner cmdRunner, SlurmRequestContext request) {
    // TODO
    return null;
  }
}
