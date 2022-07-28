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
import pipelite.executor.cmd.CmdRunner;
import pipelite.executor.describe.DescribeJobs;
import pipelite.executor.describe.DescribeJobsPollRequests;
import pipelite.executor.describe.DescribeJobsResult;
import pipelite.executor.describe.DescribeJobsResults;
import pipelite.executor.describe.context.LsfExecutorContext;
import pipelite.executor.describe.context.LsfRequestContext;
import pipelite.retryable.RetryableExternalAction;
import pipelite.service.PipeliteServices;
import pipelite.stage.executor.StageExecutorRequest;
import pipelite.stage.executor.StageExecutorResult;
import pipelite.stage.executor.StageExecutorResultAttribute;
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

  private static final int JOB_RECOVERY_LINES = 1000;
  private static final Pattern JOB_RECOVERY_EXIT_CODE_PATTERN =
      Pattern.compile("Exited with exit code (\\d+)");

  private static final String SUBMIT_CMD = "bsub";
  private static final String TERMINATE_CMD = "bkill ";
  private static final String POLL_CMD =
      "bjobs -o \"jobid stat exit_code cpu_used max_mem avg_mem exec_host delimiter='|'\" -noheader ";

  private static final Pattern SUBMIT_JOB_SUBMITTED_PATTERN =
      Pattern.compile("Job <(\\d+)\\> is submitted");
  private static final Pattern POLL_JOB_NOT_FOUND_PATTERN =
      Pattern.compile("Job <(\\d+)\\> is not found");

  private static final String POLL_JOB_STATUS_SUCCESS = "DONE";
  private static final String POLL_JOB_STATUS_ERROR = "EXIT";

  private static final int POLL_COLUMN_JOB_ID = 0;
  private static final int POLL_COLUMN_STATUS = 1;
  private static final int POLL_COLUMN_EXIT_CODE = 2;
  private static final int POLL_COLUMN_CPU_TIME = 3;
  private static final int POLL_COLUMN_MAX_MEM = 4;
  private static final int POLL_COLUMN_AVG_MEM = 5;
  private static final int POLL_COLUMN_HOST = 6;

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
    return TERMINATE_CMD + jobId;
  }

  /** Polls job execution results. */
  public static DescribeJobsResults<LsfRequestContext> pollJobs(
      CmdRunner cmdRunner, DescribeJobsPollRequests<LsfRequestContext> requests) {
    // Ignore exit code as bjobs returns 255 if some jobs are not found.
    StageExecutorResult result = cmdRunner.execute(POLL_CMD + String.join(" ", requests.jobIds));
    return extractJobResultsFromPollOutput(result.stageLog(), requests);
  }

  /** Recovers job execution result. */
  public static DescribeJobsResult<LsfRequestContext> recoverJob(
      CmdRunner cmdRunner, LsfRequestContext request) {
    String outFile = request.getOutFile();
    log.atWarning().log(
        "Recovering LSF job " + request.getJobId() + " result from output file: " + outFile);
    String str = readOutFile(cmdRunner, outFile, JOB_RECOVERY_LINES);
    return DescribeJobsResult.create(request, recoverJobUsingOutFile(str));
  }

  /**
   * Recovers stage execution result from output file. Returns null if the result could not be
   * recovered.
   */
  public static StageExecutorResult recoverJobUsingOutFile(String str) {
    if (str == null) {
      return null;
    }

    if (str.contains("Done successfully") // bhist -f result
        || str.contains("Successfully completed") // output file result
    ) {
      StageExecutorResult result = StageExecutorResult.success();
      result.attribute(StageExecutorResultAttribute.EXIT_CODE, "0");
      return result;
    }

    Integer exitCode = extractExitCodeFromOutFile(str);
    if (exitCode != null) {
      StageExecutorResult result = StageExecutorResult.executionError();
      result.attribute(StageExecutorResultAttribute.EXIT_CODE, String.valueOf(exitCode));
      return result;
    }
    return null;
  }

  public static String extractJobIdFromSubmitOutput(String str) {
    try {
      Matcher m = SUBMIT_JOB_SUBMITTED_PATTERN.matcher(str);
      if (!m.find()) {
        throw new PipeliteException("No LSF submit job id.");
      }
      return m.group(1);
    } catch (Exception ex) {
      throw new PipeliteException("No LSF submit job id.");
    }
  }

  public static String extractNotFoundJobIdFromPollOutput(String str) {
    try {
      Matcher m = POLL_JOB_NOT_FOUND_PATTERN.matcher(str);
      if (m.find()) {
        return m.group(1);
      }
      return null;
    } catch (Exception ex) {
      return null;
    }
  }

  /**
   * Extracts exit code from the output file. Returns null if the exit code could not be extracted.
   */
  public static Integer extractExitCodeFromOutFile(String str) {
    try {
      Matcher m = JOB_RECOVERY_EXIT_CODE_PATTERN.matcher(str);
      return m.find() ? Integer.valueOf(m.group(1)) : null;
    } catch (Exception ex) {
      return null;
    }
  }

  /** Extracts job results from bjobs output. */
  public static DescribeJobsResults<LsfRequestContext> extractJobResultsFromPollOutput(
      String str, DescribeJobsPollRequests<LsfRequestContext> requests) {
    DescribeJobsResults<LsfRequestContext> results = new DescribeJobsResults<>();
    for (String line : str.split("\\r?\\n")) {
      String notFoundJobId = extractNotFoundJobIdFromPollOutput(line);
      if (notFoundJobId != null) {
        log.atWarning().log("LSF job " + notFoundJobId + " was not found");
        results.add(DescribeJobsResult.create(requests, notFoundJobId, null));
      } else {
        DescribeJobsResult<LsfRequestContext> result =
            extractJobResultFromPollOutput(line, requests);
        if (result != null) {
          results.add(result);
        }
      }
    }
    return results;
  }

  /** Extracts job result from a single line of bjobs output. */
  public static DescribeJobsResult<LsfRequestContext> extractJobResultFromPollOutput(
      String str, DescribeJobsPollRequests<LsfRequestContext> requests) {
    String[] column = str.split("\\|");
    if (column.length != 7) {
      log.atWarning().log("Unexpected LSF bjobs output line: " + str);
      return null;
    }

    StageExecutorResult result;
    if (column[POLL_COLUMN_STATUS].equals(POLL_JOB_STATUS_SUCCESS)) {
      result = StageExecutorResult.success();
      result.attribute(StageExecutorResultAttribute.EXIT_CODE, String.valueOf(0));
    } else if (column[POLL_COLUMN_STATUS].equals(POLL_JOB_STATUS_ERROR)) {
      result = StageExecutorResult.executionError();
      result.attribute(
          StageExecutorResultAttribute.EXIT_CODE, String.valueOf(column[POLL_COLUMN_EXIT_CODE]));
    } else {
      result = StageExecutorResult.active();
    }

    String jobId = column[POLL_COLUMN_JOB_ID];
    result.attribute(StageExecutorResultAttribute.JOB_ID, jobId);

    if (result.isSuccess() || result.isError()) {
      result.attribute(StageExecutorResultAttribute.EXEC_HOST, column[POLL_COLUMN_HOST]);
      result.attribute(StageExecutorResultAttribute.CPU_TIME, column[POLL_COLUMN_CPU_TIME]);
      result.attribute(StageExecutorResultAttribute.MAX_MEM, column[POLL_COLUMN_MAX_MEM]);
      result.attribute(StageExecutorResultAttribute.AVG_MEM, column[POLL_COLUMN_AVG_MEM]);
    }

    return DescribeJobsResult.create(requests, jobId, result);
  }
}
