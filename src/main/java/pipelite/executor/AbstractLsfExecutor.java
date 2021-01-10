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
import com.google.common.flogger.FluentLogger;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.Getter;
import lombok.Setter;
import lombok.Value;
import lombok.extern.flogger.Flogger;
import pipelite.exception.PipeliteException;
import pipelite.exception.PipeliteInterruptedException;
import pipelite.executor.cmd.CmdRunner;
import pipelite.executor.context.*;
import pipelite.executor.task.RetryTask;
import pipelite.log.LogKey;
import pipelite.stage.executor.*;
import pipelite.stage.parameters.CmdExecutorParameters;
import pipelite.stage.parameters.ExecutorParameters;
import pipelite.time.Time;

/** Executes a command using LSF. */
@Flogger
@Getter
@Setter
@JsonIgnoreProperties({"cmdRunner"})
public abstract class AbstractLsfExecutor<T extends CmdExecutorParameters>
    extends AbstractExecutor<T> implements JsonSerializableExecutor {

  /** The command to be executed. */
  private String cmd;

  /** The JSF job id. */
  private String jobId;

  /** The LSF output file. */
  private String outFile;

  private static final LsfContextCache sharedContextCache = new LsfContextCache();

  protected LsfContextCache.Context getSharedContext() {
    return sharedContextCache.getContext((AbstractLsfExecutor<CmdExecutorParameters>) this);
  }

  @Value
  protected static class BjobsResult {
    String jobId;
    StageExecutorResult result;
  }

  private static final String OUT_FILE_SUFFIX = ".out";
  private static final Duration OUT_FILE_POLL_TIMEOUT = Duration.ofMinutes(5);
  private static final Duration OUT_FILE_POLL_FREQUENCY = Duration.ofSeconds(10);

  private static final String MKDIR_CMD = "mkdir -p ";

  protected static final String BSUB_CMD = "bsub";

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

  private static final String BHIST_CMD = "bhist -l ";
  private static final Pattern BHIST_EXIT_CODE_PATTERN =
          Pattern.compile("Exited with exit code (\\d+)");

  /**
   * Returns the submit command.
   *
   * @return the submit command.
   */
  public abstract String getSubmitCmd(StageExecutorRequest request);

  /**
   * Returns the command runner.
   *
   * @return the command runner.
   */
  public CmdRunner getCmdRunner() {
    return CmdRunner.create(getExecutorParams());
  }

  protected void beforeSubmit(StageExecutorRequest request) {}

  @Override
  public StageExecutorResult execute(StageExecutorRequest request) {
    if (jobId == null) {
      StageExecutorResult result = createWorkDir(request);
      if (result.isError()) {
        return result;
      }
      beforeSubmit(request);
      outFile = getOutFile(request, getExecutorParams());
      return submit(request);
    }
    return poll(request);
  }

  @Override
  public void terminate() {
    RetryTask.DEFAULT_FIXED.execute(r -> bkill(getCmdRunner(), jobId));
  }

  private StageExecutorResult submit(StageExecutorRequest request) {
    StageExecutorResult result =
        RetryTask.DEFAULT_FIXED.execute(r -> getCmdRunner().execute(getSubmitCmd(request)));
    if (result.isError()) {
      return result;
    }

    jobId = extractBsubJobIdSubmitted(result.getStageLog());

    logContext(log.atInfo(), request).log("LSF submit job id: %s", jobId);
    result.setResultType(StageExecutorResultType.ACTIVE);
    return result;
  }

  private StageExecutorResult poll(StageExecutorRequest request) {
    logContext(log.atFine(), request).log("Checking LSF job result using bjobs.");

    Optional<StageExecutorResult> result = getSharedContext().describeJobs.getResult(jobId);
    if (result == null || !result.isPresent()) {
      return StageExecutorResult.active();
    }
    result.get().setStageLog(readOutFile(request));
    return result.get();
  }

  private static String bjobs(CmdRunner cmdRunner, List<String> jobIds) {
    String str = String.join(" ", jobIds);
    log.atFine().log("Checking LSF job results using bjobs: " + str);
    StageExecutorResult result = cmdRunner.execute(BJOBS_CMD + str);
    if (result.isError()) {
      throw new PipeliteException("Failed to check LSF job results using bjobs.");
    }
    return result.getStageLog();
  }

  private static StageExecutorResult bhist(CmdRunner cmdRunner, String jobId) {
    log.atWarning().log("Checking LSF job result using bhist: " + jobId);
    StageExecutorResult bhistResult = cmdRunner.execute(BHIST_CMD + jobId);
    return extractBhistResult(bhistResult.getStageLog());
  }

  private static StageExecutorResult bkill(CmdRunner cmdRunner, String jobId) {
    log.atWarning().log("Terminating LSF job using bkill: " + jobId);
    return cmdRunner.execute(BKILL_CMD + jobId);
  }

  public static Map<String, StageExecutorResult> describeJobs(
      List<String> jobIds, CmdRunner cmdRunner) {
    log.atFine().log("Checking LSF job results.");

    Map<String, StageExecutorResult> results = new HashMap<>();

    String str = bjobs(cmdRunner, jobIds);

    List<BjobsResult> bjobsResults = extractBjobsResults(str);
    bjobsResults.stream()
        .filter(r -> r.getJobId() != null && r.getResult() != null)
        .forEach(r -> results.put(r.getJobId(), r.getResult()));

    // Attempt to recover missing jobs statuses. This will only ever be done
    // once and if it fails the execution will be marked as failed.
    bjobsResults.stream()
        .filter(r -> r.getJobId() != null && r.getResult() == null)
        .forEach(r -> results.put(r.getJobId(), bhist(cmdRunner, r.getJobId())));

    return results;
  }

  private StageExecutorResult createWorkDir(StageExecutorRequest request) {
    return RetryTask.DEFAULT_FIXED.execute(
        r -> getCmdRunner().execute(MKDIR_CMD + getWorkDir(request, getExecutorParams())));
  }

  protected String readOutFile(StageExecutorRequest request) {
    logContext(log.atFine(), request).log("Reading LSF out file: %s", outFile);

    // Check if the stdout file exists. The file may not be immediately available after the job
    // execution finishes.
    ZonedDateTime timeout = ZonedDateTime.now().plus(OUT_FILE_POLL_TIMEOUT);
    try {
      while (!stdoutFileExists(outFile)) {
        Time.waitUntil(OUT_FILE_POLL_FREQUENCY, timeout);
      }
    } catch (PipeliteInterruptedException ex) {
      return null;
    }

    try {
      StageExecutorResult result = writeFileToStdout(getCmdRunner(), outFile, getExecutorParams());
      return result.getStageLog();
    } catch (Exception ex) {
      log.atSevere().withCause(ex).log("Failed to read LSF out file: %s", outFile);
      return null;
    }
  }

  private boolean stdoutFileExists(String stdoutFile) {
    // Check if the stdout file exists. The file may not be immediately available after the job
    // execution finishes.
    StageExecutorResult result =
        RetryTask.DEFAULT_FIXED.execute(
            r -> getCmdRunner().execute("sh -c 'test -f " + stdoutFile + "'"));
    return result.isSuccess();
  }

  public static StageExecutorResult writeFileToStdout(
      CmdRunner cmdRunner, String stdoutFile, CmdExecutorParameters executorParams) {
    // Execute through sh required by LocalRunner to direct output to stdout/err.
    int logBytes =
        (executorParams.getLogBytes() > 0)
            ? executorParams.getLogBytes()
            : CmdExecutorParameters.DEFAULT_LOG_BYTES;
    return RetryTask.DEFAULT_FIXED.execute(
        r -> cmdRunner.execute("sh -c 'tail -c " + logBytes + " " + stdoutFile + "'"));
  }

  public static String extractBsubJobIdSubmitted(String str) {
    try {
      Matcher m = BSUB_JOB_ID_SUBMITTED_PATTERN.matcher(str);
      m.find();
      return m.group(1);
    } catch (Exception ex) {
      throw new PipeliteException("No LSF submit job id.");
    }
  }

  public static String extractBjobsJobIdNotFound(String str) {
    try {
      Matcher m = BJOBS_JOB_ID_NOT_FOUND_PATTERN.matcher(str);
      m.find();
      return m.group(1);
    } catch (Exception ex) {
      return null;
    }
  }

  public static String extractBhistExitCode(String str) {
    try {
      Matcher m = BHIST_EXIT_CODE_PATTERN.matcher(str);
      m.find();
      return m.group(1);
    } catch (Exception ex) {
      return null;
    }
  }

  public static List<BjobsResult> extractBjobsResults(String str) {
    List<BjobsResult> results = new ArrayList<>();
    for (String line : str.split("\\r?\\n")) {
      String jobId = extractBjobsJobIdNotFound(line);
      if (jobId != null) {
        results.add(new BjobsResult(jobId, null));
      } else {
        results.add(extractBjobsResult(line));
      }
    }
    return results;
  }

  public static BjobsResult extractBjobsResult(String str) {
    String[] column = str.split("\\|");
    if (column.length != 7) {
      throw new PipeliteException("Unexpected LSF bjobs output: " + str);
    }

    StageExecutorResult result = StageExecutorResult.active();
    String jobId = column[BJOBS_COLUMN_JOB_ID];
    result.getAttributes().put(StageExecutorResultAttribute.JOB_ID, jobId);

    if (column[BJOBS_COLUMN_STATUS].equals(BJOBS_STATUS_DONE)) {
      result.setResultType(StageExecutorResultType.SUCCESS);
      result.getAttributes().put(StageExecutorResultAttribute.EXIT_CODE, String.valueOf(0));
    } else if (column[BJOBS_COLUMN_STATUS].equals(BJOBS_STATUS_EXIT)) {
      result.setResultType(StageExecutorResultType.ERROR);
      result
          .getAttributes()
          .put(StageExecutorResultAttribute.EXIT_CODE, String.valueOf(column[BJOBS_COLUMN_EXIT_CODE]));
    }

    if (!result.isActive()) {
      result.getAttributes().put(StageExecutorResultAttribute.EXEC_HOST, column[BJOBS_COLUMN_HOST]);
      result.getAttributes().put(StageExecutorResultAttribute.CPU_TIME, column[BJOBS_COLUMN_CPU_TIME]);
      result.getAttributes().put(StageExecutorResultAttribute.MAX_MEM, column[BJOBS_COLUMN_MAX_MEM]);
      result.getAttributes().put(StageExecutorResultAttribute.AVG_MEM, column[BJOBS_COLUMN_AVG_MEM]);
    }

    return new BjobsResult(jobId, result);
  }

  public static StageExecutorResult extractBhistResult(String str) {
    if (str == null) {
      return StageExecutorResult.error();
    }
    if (str.contains("Done successfully")) {
      StageExecutorResult result = StageExecutorResult.success();
      result.getAttributes().put(StageExecutorResultAttribute.EXIT_CODE, "0");
      return result;
    }
    if (str.contains("Completed <exit>")) {
      int exitCode = Integer.valueOf(extractBhistExitCode(str));
      StageExecutorResult result = StageExecutorResult.error();
      result.getAttributes().put(StageExecutorResultAttribute.EXIT_CODE, String.valueOf(exitCode));
      return result;
    }
    return StageExecutorResult.error();
  }

  protected static void addArgument(StringBuilder cmd, String argument) {
    cmd.append(" ");
    cmd.append(argument);
  }

  public static <T extends CmdExecutorParameters> Path getWorkDir(
      StageExecutorRequest request, T params) {
    Path path;
    if (params != null && params.getWorkDir() != null) {
      path =
          ExecutorParameters.validatePath(params.getWorkDir(), "workDir")
              .resolve("pipelite")
              .normalize();
    } else {
      path = Paths.get("pipelite");
    }
    return path.resolve(request.getPipelineName()).resolve(request.getProcessId());
  }

  public void setOutFile(String outFile) {
    this.outFile = outFile;
  }

  /**
   * Returns the output file in the working directory for stdout and stderr.
   *
   * @param request the pipeline name
   * @param params the execution parameters
   * @return the output file path
   */
  public static <T extends CmdExecutorParameters> String getOutFile(
      StageExecutorRequest request, T params) {
    return getWorkDir(request, params)
        .resolve(request.getStage().getStageName() + OUT_FILE_SUFFIX)
        .toString();
  }

  /**
   * Returns the output file.
   *
   * @return the output file
   */
  public String getOutFile() {
    return outFile;
  }

  protected FluentLogger.Api logContext(FluentLogger.Api log, StageExecutorRequest request) {
    return log.with(LogKey.PIPELINE_NAME, request.getPipelineName())
        .with(LogKey.PROCESS_ID, request.getProcessId())
        .with(LogKey.STAGE_NAME, request.getStage().getStageName());
  }
}
