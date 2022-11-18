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
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.flogger.Flogger;
import pipelite.exception.PipeliteException;
import pipelite.executor.describe.DescribeJobs;
import pipelite.executor.describe.context.executor.LsfExecutorContext;
import pipelite.executor.describe.context.request.LsfRequestContext;
import pipelite.retryable.RetryableExternalAction;
import pipelite.service.PipeliteServices;
import pipelite.stage.executor.StageExecutorRequest;
import pipelite.stage.executor.StageExecutorResult;
import pipelite.stage.parameters.AbstractLsfExecutorParameters;
import pipelite.stage.path.LsfLogFilePathResolver;

/** Executes a command using LSF. */
@Flogger
@Getter
@Setter
@JsonIgnoreProperties({"cmdRunner"})
public abstract class AbstractLsfExecutor<T extends AbstractLsfExecutorParameters>
    extends AsyncCmdExecutor<T, LsfRequestContext, LsfExecutorContext>
    implements JsonSerializableExecutor {

  private static final String SUBMIT_CMD = "bsub";
  private static final String TERMINATE_CMD = "bkill";
  private static final Pattern SUBMIT_JOB_ID_PATTERN =
      Pattern.compile("Job <(\\d+)\\> is submitted");

  public AbstractLsfExecutor() {
    super("LSF", new LsfLogFilePathResolver());
  }

  @Override
  protected LsfRequestContext prepareRequestContext() {
    return new LsfRequestContext(getJobId(), outFile);
  }

  @Override
  protected DescribeJobs<LsfRequestContext, LsfExecutorContext> prepareDescribeJobs(
      PipeliteServices pipeliteServices) {
    return pipeliteServices
        .jobs()
        .lsf()
        .getDescribeJobs((AbstractLsfExecutor<AbstractLsfExecutorParameters>) this);
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

    String logDir = logFilePathResolver.placeholderPath().dir(request);
    String logFileName = logFilePathResolver.fileName(request);
    if (logDir != null) {
      addCmdArgument(cmd, "-outdir");
      addCmdArgument(cmd, "\"" + logDir + "\"");
      addCmdArgument(cmd, "-cwd");
      addCmdArgument(cmd, "\"" + logDir + "\"");
    }
    addCmdArgument(cmd, "-oo");
    addCmdArgument(cmd, logFileName);
    return cmd;
  }

  protected static void addCmdArgument(StringBuilder cmd, String argument) {
    cmd.append(" ");
    cmd.append(argument);
  }

  @Override
  protected SubmitJobResult submitJob() {
    StageExecutorRequest request = getRequest();
    outFile = logFilePathResolver.resolvedPath().file(request);
    StageExecutorResult result =
        RetryableExternalAction.execute(() -> getCmdRunner().execute(getSubmitCmd(request)));
    String jobId = null;
    if (!result.isError()) {
      jobId = extractJobIdFromSubmitOutput(result.stageLog());
      logContext(log.atInfo(), request).log("Submitted LSF job " + jobId);
    }
    return new SubmitJobResult(jobId, result);
  }

  @Override
  protected void terminateJob() {
    log.atWarning().log("Terminating LSF job " + getJobId());
    getCmdRunner().execute(getTerminateCmd(getJobId()));
  }

  static String getTerminateCmd(String jobId) {
    return TERMINATE_CMD + " " + jobId;
  }

  public static String extractJobIdFromSubmitOutput(String str) {
    try {
      Matcher m = SUBMIT_JOB_ID_PATTERN.matcher(str);
      if (!m.find()) {
        throw new PipeliteException("No LSF submit job id.");
      }
      return m.group(1);
    } catch (Exception ex) {
      throw new PipeliteException("No LSF submit job id.");
    }
  }
}
