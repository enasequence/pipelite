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

import com.google.common.flogger.FluentLogger;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.flogger.Flogger;
import pipelite.exception.PipeliteInterruptedException;
import pipelite.executor.cmd.CmdRunner;
import pipelite.executor.cmd.CmdRunnerResult;
import pipelite.log.LogKey;
import pipelite.stage.executor.*;
import pipelite.stage.executor.InternalError;
import pipelite.stage.parameters.CmdExecutorParameters;
import pipelite.stage.parameters.ExecutorParameters;
import pipelite.time.Time;

/** Executes a command using LSF. */
@Flogger
@Getter
@Setter
public abstract class AbstractLsfExecutor<T extends CmdExecutorParameters> extends CmdExecutor<T>
    implements JsonSerializableExecutor {

  private String jobId;
  private ZonedDateTime startTime;
  private String outFile;

  private static final String OUT_FILE_SUFFIX = ".out";
  private static final Duration OUT_FILE_POLL_TIMEOUT = Duration.ofMinutes(5);
  private static final Duration OUT_FILE_POLL_FREQUENCY = Duration.ofSeconds(10);

  protected static final String BSUB_CMD = "bsub";
  private static final String BJOBS_STANDARD_CMD = "bjobs -l ";
  private static final String BJOBS_CUSTOM_CMD =
      "bjobs -o \"stat exit_code cpu_used max_mem avg_mem exec_host delimiter='|'\" -noheader ";
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
  private static final int BJOBS_STATUS = 0;
  private static final int BJOBS_EXIT_CODE = 1;
  private static final int BJOBS_CPU_TIME = 2;
  private static final int BJOBS_MAX_MEM = 3;
  private static final int BJOBS_AVG_MEM = 4;
  private static final int BJOBS_HOST = 5;

  @Override
  public final String getDispatcherCmd(StageExecutorRequest request) {
    return getSubmitCmd(request);
  }

  protected abstract String getSubmitCmd(StageExecutorRequest request);

  protected void beforeSubmit(StageExecutorRequest request) {}

  protected void afterSubmit(StageExecutorRequest request) {}

  @Override
  public StageExecutorResult execute(StageExecutorRequest request) {
    if (jobId == null) {
      try {
        startTime = ZonedDateTime.now();
        StageExecutorResult result = createWorkDir(request);
        if (result.isError()) {
          return result;
        }
        beforeSubmit(request);
        result = submit(request);
        try {
          afterSubmit(request);
        } catch (Exception ex) {
          logContext(log.atSevere().withCause(ex), request)
              .log("Unexpected exception after submit.");
        }
        return result;
      } catch (Exception ex) {
        return StageExecutorResult.internalError(InternalError.SUBMIT);
      }
    }

    return poll(request);
  }

  private StageExecutorResult createWorkDir(StageExecutorRequest request) {
    String cmd = MKDIR_CMD + getWorkDir(request, getExecutorParams());
    return cmdRunner.execute(cmd, getExecutorParams()).getStageExecutorResult(cmd);
  }

  private StageExecutorResult submit(StageExecutorRequest request) {

    outFile = getOutFile(request, getExecutorParams());

    StageExecutorResult result = super.execute(request);
    if (result.isError()) {
      return result;
    }
    String stageLog = result.getStageLog();
    jobId = extractBsubJobIdSubmitted(stageLog);

    if (jobId == null) {
      logContext(log.atSevere(), request).log("No LSF submit job id.");
      result = StageExecutorResult.internalError(InternalError.SUBMIT);
    } else {
      logContext(log.atInfo(), request).log("LSF submit job id: %s", jobId);
      result.setResultType(StageExecutorResultType.ACTIVE);
    }
    return result;
  }

  private StageExecutorResult poll(StageExecutorRequest request) {

    Duration timeout = getExecutorParams().getTimeout();

    if (timeout != null && ZonedDateTime.now().isAfter(startTime.plus(timeout))) {
      logContext(log.atSevere(), request).log("Maximum run time exceeded. Killing LSF job.");
      try {
        cmdRunner.execute(BKILL_CMD + jobId, getExecutorParams());
      } catch (Exception ex) {
        logContext(log.atSevere().withCause(ex), request)
            .log("Unexpected exception when killing LSF job %s", getJobId());
        return StageExecutorResult.internalError(InternalError.TERMINATE);
      }
      reset();
      return StageExecutorResult.internalError(InternalError.TIMEOUT);
    }

    logContext(log.atFine(), request).log("Checking LSF job result using bjobs.");

    CmdRunnerResult bjobsCustomCmdRunnerResult =
        cmdRunner.execute(BJOBS_CUSTOM_CMD + jobId, getExecutorParams());

    StageExecutorResult result;

    if (!extractBjobsJobIdNotFound(bjobsCustomCmdRunnerResult.getStdout())) {
      result = extractBjobsCustomResult(bjobsCustomCmdRunnerResult.getStdout());
      if (result.isActive()) {
        return result;
      }
    } else {
      logContext(log.atFine(), request).log("Checking LSF job result using bhist.");

      CmdRunnerResult bhistCmdRunnerResult =
          cmdRunner.execute(BHIST_CMD + jobId, getExecutorParams());

      result = extractBjobsStandardResult(bhistCmdRunnerResult.getStdout());
      if (result == null) {
        reset();
        result = StageExecutorResult.error();
      }
    }

    String stdout = getStdout(request);
    result.setStageLog(stdout);
    return result;
  }

  protected String getStdout(StageExecutorRequest request) {

    // Check if the stdout file exists. The file may not be immediately available after the job
    // execution finishes.

    ZonedDateTime waitUntil = ZonedDateTime.now().plus(OUT_FILE_POLL_TIMEOUT);
    try {
      while (!stdoutFileExists(cmdRunner, outFile)) {
        Time.waitUntil(OUT_FILE_POLL_FREQUENCY, waitUntil);
      }
    } catch (PipeliteInterruptedException ex) {
      return null;
    }

    logContext(log.atFine(), request).log("Reading stdout file: %s", outFile);

    try {
      CmdRunnerResult stdoutCmdRunnerResult =
          writeFileToStdout(cmdRunner, outFile, getExecutorParams());
      return stdoutCmdRunnerResult.getStdout();
    } catch (Exception ex) {
      log.atSevere().withCause(ex).log("Failed to read stdout file: %s", outFile);
      return null;
    }
  }

  private boolean stdoutFileExists(CmdRunner cmdRunner, String stdoutFile) {
    // Check if the stdout file exists. The file may not be immediately available after the job
    // execution finishes.
    return 0
        == cmdRunner
            .execute("sh -c 'test -f " + stdoutFile + "'", getExecutorParams())
            .getExitCode();
  }

  private void reset() {
    jobId = null;
    startTime = null;
  }

  public static CmdRunnerResult writeFileToStdout(
      CmdRunner cmdRunner, String stdoutFile, CmdExecutorParameters executorParams) {
    // Execute through sh required by LocalRunner to direct output to stdout/err.
    return cmdRunner.execute("sh -c 'cat " + stdoutFile + "'", executorParams);
  }

  public static CmdRunnerResult writeFileToStderr(
      CmdRunner cmdRunner, String stderrFile, CmdExecutorParameters executorParams) {
    // Execute through sh required by LocalRunner to direct output to stdout/err.
    return cmdRunner.execute("sh -c 'cat " + stderrFile + " 1>&2'", executorParams);
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

  public static boolean extractBjobsJobIdNotFound(String str) {
    try {
      Matcher m = BJOBS_JOB_ID_NOT_FOUND_PATTERN.matcher(str);
      return m.find();
    } catch (Exception ex) {
      return false;
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

  private StageExecutorResult extractBjobsCustomResult(String str) {
    String[] bjobs = str.split("\\|");

    int exitCode = 0;
    StageExecutorResult result = null;

    if (bjobs[BJOBS_STATUS].equals(BJOBS_STATUS_DONE)) {
      result = StageExecutorResult.success();
    } else if (bjobs[BJOBS_STATUS].equals(BJOBS_STATUS_EXIT)) {
      exitCode = Integer.valueOf(bjobs[BJOBS_EXIT_CODE]);
      result = StageExecutorResult.error();
    }

    if (result != null) {
      result.getAttributes().put(StageExecutorResultAttribute.EXIT_CODE, String.valueOf(exitCode));
      result.getAttributes().put(StageExecutorResultAttribute.EXEC_HOST, bjobs[BJOBS_HOST]);
      result.getAttributes().put(StageExecutorResultAttribute.CPU_TIME, bjobs[BJOBS_CPU_TIME]);
      result.getAttributes().put(StageExecutorResultAttribute.MAX_MEM, bjobs[BJOBS_MAX_MEM]);
      result.getAttributes().put(StageExecutorResultAttribute.AVG_MEM, bjobs[BJOBS_AVG_MEM]);
      return result;
    }

    return StageExecutorResult.active();
  }

  private static StageExecutorResult extractBjobsStandardResult(String str) {
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
