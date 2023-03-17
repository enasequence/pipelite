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
import java.time.Duration;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.flogger.Flogger;
import pipelite.exception.PipeliteException;
import pipelite.executor.describe.DescribeJobs;
import pipelite.executor.describe.context.executor.SlurmExecutorContext;
import pipelite.executor.describe.context.request.SlurmRequestContext;
import pipelite.retryable.Retry;
import pipelite.service.PipeliteServices;
import pipelite.stage.executor.StageExecutorRequest;
import pipelite.stage.executor.StageExecutorResult;
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

  private static final String SUBMIT_CMD = "sbatch";
  private static final String TERMINATE_CMD = "scancel";
  private static final Pattern SUBMIT_JOB_ID_PATTERN =
      Pattern.compile("Submitted batch job (\\d+)");

  public static final Duration DEFAULT_TIMEOUT = Duration.ofDays(7);

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

  protected StringBuilder getSharedSubmitCmd(StageExecutorRequest request) {
    StringBuilder cmd = new StringBuilder();
    cmd.append(SUBMIT_CMD);
    return cmd;
  }

  @Override
  protected SubmitJobResult submitJob() {
    StageExecutorRequest request = getRequest();
    outFile = logFilePathResolver.resolvedPath().file(request);
    StageExecutorResult result = Retry.DEFAULT.execute(getCmdRunner(), getSubmitCmd(request));
    String jobId = null;
    if (!result.isError()) {
      jobId = extractJobIdFromSubmitOutput(result.stdOut());
      logContext(log.atInfo(), request).log("Submitted SLURM job " + jobId);
    }
    return new SubmitJobResult(jobId, result);
  }

  @Override
  protected void terminateJob() {
    log.atWarning().log("Terminating SLURM job " + getJobId());
    getCmdRunner().execute(getTerminateCmd(getJobId()));
  }

  static String getTerminateCmd(String jobId) {
    return TERMINATE_CMD + " " + jobId;
  }

  public static String extractJobIdFromSubmitOutput(String str) {
    try {
      Matcher m = SUBMIT_JOB_ID_PATTERN.matcher(str);
      if (!m.find()) {
        throw new PipeliteException("No SLURM submit job id.");
      }
      return m.group(1);
    } catch (Exception ex) {
      throw new PipeliteException("No SLURM submit job id.");
    }
  }
}
