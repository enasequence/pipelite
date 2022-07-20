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
package pipelite.executor;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
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

import java.util.regex.Matcher;
import java.util.regex.Pattern;

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

  private static final String BSUB_CMD = "bsub";
  private static final String BKILL_CMD = "bkill ";
  private static final String BJOBS_CMD =
      "bjobs -o \"jobid stat exit_code cpu_used max_mem avg_mem exec_host delimiter='|'\" -noheader ";

  private static final Pattern BSUB_JOB_ID_SUBMITTED_PATTERN =
      Pattern.compile("Job <(\\d+)\\> is submitted");
  private static final Pattern BJOBS_JOB_ID_NOT_FOUND_PATTERN =
      Pattern.compile("Job <(\\d+)\\> is not found");

  private static final String BJOBS_STATUS_DONE = "DONE";
  private static final String BJOBS_STATUS_EXIT = "EXIT";

  private static final int BJOBS_COLUMN_JOB_ID = 0;
  private static final int BJOBS_COLUMN_STATUS = 1;
  private static final int BJOBS_COLUMN_EXIT_CODE = 2;
  private static final int BJOBS_COLUMN_CPU_TIME = 3;
  private static final int BJOBS_COLUMN_MAX_MEM = 4;
  private static final int BJOBS_COLUMN_AVG_MEM = 5;
  private static final int BJOBS_COLUMN_HOST = 6;

  public AbstractLsfExecutor() {
    super(new LsfLogFilePathResolver());
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
    cmd.append(BSUB_CMD);

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
      jobId = extractJobIdFromBsubOutput(result.getStageLog());
      logContext(log.atInfo(), request).log("Submitted LSF job " + jobId);
    }
    return new SubmitJobResult(jobId, result);
  }

  @Override
  protected void terminateJob() {
    log.atWarning().log("Terminating LSF job " + getJobId());
    getCmdRunner().execute(BKILL_CMD + getCmd());
  }

  /** Polls job execution results. */
  public static DescribeJobsResults<LsfRequestContext> pollJobs(
      CmdRunner cmdRunner, DescribeJobsPollRequests<LsfRequestContext> requests) {
    // Ignore exit code as bjobs returns 255 if some jobs are not found.
    StageExecutorResult result = cmdRunner.execute(BJOBS_CMD + String.join(" ", requests.jobIds));
    return extractJobResultsFromBjobsOutput(result.getStageLog(), requests);
  }

  /** Recovers job execution result. */
  public static DescribeJobsResult<LsfRequestContext> recoverJob(
      CmdRunner cmdRunner, LsfRequestContext request) {
    String outFile = request.getOutFile();
    log.atWarning().log("Recovering LSF job result from output file: " + outFile);
    String str = readOutFile(cmdRunner, outFile, JOB_RECOVERY_LINES);
    return new DescribeJobsResult<>(request, recoverJobUsingOutFile(str));
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
      result.addAttribute(StageExecutorResultAttribute.EXIT_CODE, "0");
      return result;
    }

    Integer exitCode = extractExitCodeFromOutFile(str);
    if (exitCode != null) {
      StageExecutorResult result = StageExecutorResult.error();
      result.addAttribute(StageExecutorResultAttribute.EXIT_CODE, String.valueOf(exitCode));
      return result;
    }
    return null;
  }

  public static String extractJobIdFromBsubOutput(String str) {
    try {
      Matcher m = BSUB_JOB_ID_SUBMITTED_PATTERN.matcher(str);
      if (!m.find()) {
        throw new PipeliteException("No LSF submit job id.");
      }
      return m.group(1);
    } catch (Exception ex) {
      throw new PipeliteException("No LSF submit job id.");
    }
  }

  public static String extractNotFoundJobIdFromBjobsOutput(String str) {
    try {
      Matcher m = BJOBS_JOB_ID_NOT_FOUND_PATTERN.matcher(str);
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
  public static DescribeJobsResults<LsfRequestContext> extractJobResultsFromBjobsOutput(
      String str, DescribeJobsPollRequests<LsfRequestContext> requests) {
    DescribeJobsResults<LsfRequestContext> results = new DescribeJobsResults<>();
    for (String line : str.split("\\r?\\n")) {
      String notFoundJobId = extractNotFoundJobIdFromBjobsOutput(line);
      if (notFoundJobId != null) {
        log.atWarning().log("LSF bsubs job not found: " + notFoundJobId);
        results.add(new DescribeJobsResult<>(requests, notFoundJobId, null));
      } else {
        DescribeJobsResult<LsfRequestContext> result = extractJobResultFromBjobsOutput(line, requests);
        if (result != null) {
          results.add(result);
        }
      }
    }
    return results;
  }

  /** Extracts job result from a single line of bjobs output. */
  public static DescribeJobsResult<LsfRequestContext> extractJobResultFromBjobsOutput(
      String str, DescribeJobsPollRequests<LsfRequestContext> requests) {
    String[] column = str.split("\\|");
    if (column.length != 7) {
      log.atWarning().log("Unexpected bjobs output line: " + str);
      return null;
    }

    StageExecutorResult result;
    if (column[BJOBS_COLUMN_STATUS].equals(BJOBS_STATUS_DONE)) {
      result = StageExecutorResult.success();
      result.addAttribute(StageExecutorResultAttribute.EXIT_CODE, String.valueOf(0));
    } else if (column[BJOBS_COLUMN_STATUS].equals(BJOBS_STATUS_EXIT)) {
      result = StageExecutorResult.error();
      result.addAttribute(
          StageExecutorResultAttribute.EXIT_CODE, String.valueOf(column[BJOBS_COLUMN_EXIT_CODE]));
    } else {
      result = StageExecutorResult.active();
    }

    String jobId = column[BJOBS_COLUMN_JOB_ID];
    result.addAttribute(StageExecutorResultAttribute.JOB_ID, jobId);

    if (result.isSuccess() || result.isError()) {
      result.addAttribute(StageExecutorResultAttribute.EXEC_HOST, column[BJOBS_COLUMN_HOST]);
      result.addAttribute(StageExecutorResultAttribute.CPU_TIME, column[BJOBS_COLUMN_CPU_TIME]);
      result.addAttribute(StageExecutorResultAttribute.MAX_MEM, column[BJOBS_COLUMN_MAX_MEM]);
      result.addAttribute(StageExecutorResultAttribute.AVG_MEM, column[BJOBS_COLUMN_AVG_MEM]);
    }

    return new DescribeJobsResult<>(requests, jobId, result);
  }
}
