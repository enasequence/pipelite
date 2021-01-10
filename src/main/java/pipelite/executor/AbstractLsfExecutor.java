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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.Getter;
import lombok.Setter;
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

  private static final LsfContextCache sharedContextCache =
      new LsfContextCache();

  protected LsfContextCache.Context getSharedContext() {
    return sharedContextCache.getContext((AbstractLsfExecutor<CmdExecutorParameters>) this);
  }

  private static final String OUT_FILE_SUFFIX = ".out";
  private static final Duration OUT_FILE_POLL_TIMEOUT = Duration.ofMinutes(5);
  private static final Duration OUT_FILE_POLL_FREQUENCY = Duration.ofSeconds(10);

  protected static final String BSUB_CMD = "bsub";
  private static final String BJOBS_STANDARD_CMD = "bjobs -l ";
  private static final String BJOBS_CUSTOM_CMD =
      "bjobs -o \"jobid stat exit_code cpu_used max_mem avg_mem exec_host delimiter='|'\" -noheader ";
  private static final String BHIST_CMD = "bhist -l ";
  private static final String BKILL_CMD = "bkill ";
  private static final String MKDIR_CMD = "mkdir -p ";
  private static final Pattern BSUB_JOB_ID_SUBMITTED_PATTERN =
      Pattern.compile("Job <(\\d+)\\> is submitted");
  private static final Pattern BJOBS_JOB_ID_NOT_FOUND_PATTERN =
      Pattern.compile("Job <(\\d+)\\> is not found");
  private static final Pattern BJOBS_EXIT_CODE_PATTERN =
      Pattern.compile("Exited with exit code (\\d+)");
  private static final String BJOBS_STATUS_DONE = "DONE";
  private static final String BJOBS_STATUS_EXIT = "EXIT";
  private static final int BJOBS_JOB_ID = 0;
  private static final int BJOBS_STATUS = 1;
  private static final int BJOBS_EXIT_CODE = 2;
  private static final int BJOBS_CPU_TIME = 3;
  private static final int BJOBS_MAX_MEM = 4;
  private static final int BJOBS_AVG_MEM = 5;
  private static final int BJOBS_HOST = 6;

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
    RetryTask.DEFAULT_FIXED_RETRY.execute(r -> getCmdRunner().execute(BKILL_CMD + jobId));
  }

  private StageExecutorResult submit(StageExecutorRequest request) {
    StageExecutorResult result =
        RetryTask.DEFAULT_FIXED_RETRY.execute(r -> getCmdRunner().execute(getSubmitCmd(request)));
    if (result.isError()) {
      return result;
    }

    jobId = extractBsubJobIdSubmitted(result.getStageLog());
    if (jobId == null) {
      throw new PipeliteException("No LSF submit job id.");
    }

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

  public static Map<String, StageExecutorResult> describeJobs(
      List<String> jobIds, CmdRunner cmdRunner) {
    log.atFine().log("Checking LSF job results using bjobs.");

    Map<String, StageExecutorResult> results = new HashMap<>();

    StageExecutorResult bjobsResult =
        cmdRunner.execute(BJOBS_CUSTOM_CMD + String.join(" ", jobIds));

    if (bjobsResult.isError()) {
      throw new PipeliteException("Failed to check LSF job results using bjobs.");
    }
    for (String line : bjobsResult.getStageLog().split("\\r?\\n")) {
      String jobIdNotFound = extractBjobsJobIdNotFound(line);
      if (jobIdNotFound == null) {
        StageExecutorResult result = extractBjobsCustomResult(line);
        if (result != null && !result.isActive()) {
          String jobId = result.getAttribute(StageExecutorResultAttribute.JOB_ID);
          if (jobId != null) {
            results.put(jobId, result);
          } else {
            log.atSevere().log("Missing LSF bjobs job id: " + line);
          }
        }
      } else {
        log.atWarning().log("Checking LSF job result using bhist: " + jobIdNotFound);

        StageExecutorResult bhistResult = cmdRunner.execute(BHIST_CMD + jobIdNotFound);
        StageExecutorResult result = extractBjobsStandardResult(bhistResult.getStageLog());
        if (result == null) {
          results.put(jobIdNotFound, StageExecutorResult.error());
        } else {
          results.put(jobIdNotFound, result);
        }
      }
    }

    return results;
  }

  private StageExecutorResult createWorkDir(StageExecutorRequest request) {
    return RetryTask.DEFAULT_FIXED_RETRY.execute(
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
        RetryTask.DEFAULT_FIXED_RETRY.execute(
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
    return RetryTask.DEFAULT_FIXED_RETRY.execute(
        r -> cmdRunner.execute("sh -c 'tail -c " + logBytes + " " + stdoutFile + "'"));
  }

  public static String extractBsubJobIdSubmitted(String str) {
    try {
      Matcher m = BSUB_JOB_ID_SUBMITTED_PATTERN.matcher(str);
      m.find();
      return m.group(1);
    } catch (Exception ex) {
      return null;
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

  public static String extractBjobsExitCode(String str) {
    try {
      Matcher m = BJOBS_EXIT_CODE_PATTERN.matcher(str);
      m.find();
      return m.group(1);
    } catch (Exception ex) {
      return null;
    }
  }

  private static StageExecutorResult extractBjobsCustomResult(String str) {
    String[] column = str.split("\\|");
    if (column.length != 7) {
      log.atSevere().log("Unexpected LSF bjobs output: " + str);
      return null;
    }

    StageExecutorResult result = null;
    Integer exitCode = null;
    if (column[BJOBS_STATUS].equals(BJOBS_STATUS_DONE)) {
      result = StageExecutorResult.success();
      exitCode = 0;
    } else if (column[BJOBS_STATUS].equals(BJOBS_STATUS_EXIT)) {
      result = StageExecutorResult.error();
      exitCode = Integer.valueOf(column[BJOBS_EXIT_CODE]);
    }

    if (result != null) {
      result.getAttributes().put(StageExecutorResultAttribute.JOB_ID, column[BJOBS_JOB_ID]);
      result.getAttributes().put(StageExecutorResultAttribute.EXIT_CODE, String.valueOf(exitCode));
      result.getAttributes().put(StageExecutorResultAttribute.EXEC_HOST, column[BJOBS_HOST]);
      result.getAttributes().put(StageExecutorResultAttribute.CPU_TIME, column[BJOBS_CPU_TIME]);
      result.getAttributes().put(StageExecutorResultAttribute.MAX_MEM, column[BJOBS_MAX_MEM]);
      result.getAttributes().put(StageExecutorResultAttribute.AVG_MEM, column[BJOBS_AVG_MEM]);
      return result;
    }

    return StageExecutorResult.active();
  }

  private static StageExecutorResult extractBjobsStandardResult(String str) {
    if (str == null) {
      return null;
    }
    if (str.contains("Done successfully")) {
      StageExecutorResult result = StageExecutorResult.success();
      result.getAttributes().put(StageExecutorResultAttribute.EXIT_CODE, "0");
      return result;
    }
    if (str.contains("Completed <exit>")) {
      int exitCode = Integer.valueOf(extractBjobsExitCode(str));
      StageExecutorResult result = StageExecutorResult.error();
      result.getAttributes().put(StageExecutorResultAttribute.EXIT_CODE, String.valueOf(exitCode));
      return result;
    }
    return null;
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
