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
import pipelite.stage.*;
import pipelite.stage.executor.JsonSerializableExecutor;
import pipelite.stage.executor.StageExecutorResult;
import pipelite.stage.executor.StageExecutorResultAttribute;
import pipelite.stage.executor.StageExecutorResultType;
import pipelite.stage.parameters.LsfExecutorParameters;
import pipelite.time.Time;

/** Executes a command using LSF. */
@Flogger
@Getter
@Setter
public class LsfExecutor extends CmdExecutor<LsfExecutorParameters>
    implements JsonSerializableExecutor {

  private String jobId;
  private String stdoutFile;
  private ZonedDateTime startTime;

  private static final String BSUB_CMD = "bsub";
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
  private static final Duration STDOUT_FILE_MAX_WAIT_TIME = Duration.ofMinutes(5);
  private static final Duration STDOUT_FILE_POLL_WAIT_TIME = Duration.ofSeconds(10);

  @Override
  public StageExecutorResult execute(String pipelineName, String processId, Stage stage) {
    if (jobId == null) {
      return submit(pipelineName, processId, stage);
    }

    return poll(pipelineName, processId, stage);
  }

  private StageExecutorResult submit(String pipelineName, String processId, Stage stage) {
    startTime = ZonedDateTime.now();

    if (!getWorkDir(pipelineName, processId).isEmpty()) {
      cmdRunner.execute(MKDIR_CMD + getWorkDir(pipelineName, processId), getExecutorParams());
    }

    StageExecutorResult result = super.execute(pipelineName, processId, stage);
    String stdout = result.getStdout();
    String stderr = result.getStderr();
    jobId = extractBsubJobIdSubmitted(stdout);
    if (jobId == null) {
      jobId = extractBsubJobIdSubmitted(stderr);
    }

    if (jobId == null) {
      logContext(log.atSevere(), pipelineName, processId, stage).log("No LSF submit job id.");
      result.setResultType(StageExecutorResultType.ERROR);
      result.setInternalError(StageExecutorResult.InternalError.CMD_SUBMIT);
    } else {
      result.setResultType(StageExecutorResultType.ACTIVE);
    }
    return result;
  }

  private StageExecutorResult poll(String pipelineName, String processId, Stage stage) {

    Duration timeout = getExecutorParams().getTimeout();

    if (timeout != null && ZonedDateTime.now().isAfter(startTime.plus(timeout))) {
      logContext(log.atSevere(), pipelineName, processId, stage)
          .log("Maximum run time exceeded. Killing LSF job.");
      cmdRunner.execute(BKILL_CMD + jobId, getExecutorParams());
      reset();
      StageExecutorResult result = StageExecutorResult.error();
      result.setInternalError(StageExecutorResult.InternalError.CMD_TIMEOUT);
      return result;
    }

    logContext(log.atFine(), pipelineName, processId, stage)
        .log("Checking LSF job result using bjobs.");

    CmdRunnerResult bjobsCustomCmdRunnerResult =
        cmdRunner.execute(BJOBS_CUSTOM_CMD + jobId, getExecutorParams());

    StageExecutorResult result;

    if (!extractBjobsJobIdNotFound(bjobsCustomCmdRunnerResult.getStdout())) {
      result = extractBjobsCustomResult(bjobsCustomCmdRunnerResult.getStdout());
      if (result.isActive()) {
        return result;
      }
    } else {
      logContext(log.atFine(), pipelineName, processId, stage)
          .log("Checking LSF job result using bhist.");

      CmdRunnerResult bhistCmdRunnerResult =
          cmdRunner.execute(BHIST_CMD + jobId, getExecutorParams());

      result = extractBjobsStandardResult(bhistCmdRunnerResult.getStdout());
      if (result == null) {
        reset();
        result = StageExecutorResult.error();
      }
    }

    // Check if the stdout file exists. The file may not be immediately available after the job
    // execution finishes.

    ZonedDateTime stdoutFileWaitStart = ZonedDateTime.now();
    while (stdoutFileWaitStart.plus(STDOUT_FILE_MAX_WAIT_TIME).isAfter(ZonedDateTime.now())
        && !stdoutFileExists(cmdRunner, stdoutFile)) {

      if (!Time.wait(STDOUT_FILE_POLL_WAIT_TIME)) {
        throw new PipeliteInterruptedException("LSF command executor was interrupted");
      }
    }

    logContext(log.atFine(), pipelineName, processId, stage)
        .log("Reading stdout file: %s", stdoutFile);

    try {
      CmdRunnerResult stdoutCmdRunnerResult =
          writeFileToStdout(cmdRunner, stdoutFile, getExecutorParams());
      result.setStdout(stdoutCmdRunnerResult.getStdout());
    } catch (Exception ex) {
      log.atSevere().withCause(ex).log("Failed to read stdout file: %s", stdoutFile);
    }

    return result;
  }

  private void reset() {
    jobId = null;
    stdoutFile = null;
    startTime = null;
  }

  @Override
  public final String getPrefixCmd(String pipelineName, String processId, Stage stage) {

    StringBuilder cmd = new StringBuilder();
    cmd.append(BSUB_CMD);

    stdoutFile = getOutFile(pipelineName, processId, stage.getStageName(), "stdout");

    // Write both stderr and stdout to the stdout file.

    addArgument(cmd, "-oo");
    addArgument(cmd, stdoutFile);

    Integer cores = getExecutorParams().getCores();
    if (cores != null && cores > 0) {
      addArgument(cmd, "-n");
      addArgument(cmd, Integer.toString(cores));
    }

    Integer memory = getExecutorParams().getMemory();
    Duration memoryTimeout = getExecutorParams().getMemoryTimeout();
    if (memory != null && memory > 0) {
      addArgument(cmd, "-M");
      addArgument(cmd, Integer.toString(memory) + "M"); // Megabytes
      addArgument(cmd, "-R");
      addArgument(
          cmd,
          "\"rusage[mem="
              + memory
              + ((memoryTimeout == null || memoryTimeout.toMinutes() < 0)
                  ? "M" // Megabytes
                  : "M:duration=" + memoryTimeout.toMinutes())
              + "]\"");
    }

    Duration timeout = getExecutorParams().getTimeout();
    if (timeout != null) {
      if (timeout.toMinutes() == 0) {
        timeout = Duration.ofMinutes(1);
      }
      addArgument(cmd, "-W");
      addArgument(cmd, String.valueOf(timeout.toMinutes()));
    }

    String queue = getExecutorParams().getQueue();
    if (queue != null) {
      addArgument(cmd, "-q");
      addArgument(cmd, queue);
    }

    return cmd.toString();
  }

  public boolean stdoutFileExists(CmdRunner cmdRunner, String stdoutFile) {
    // Check if the stdout file exists. The file may not be immediately available after the job
    // execution finishes.
    return 0
        == cmdRunner
            .execute("sh -c 'test -f " + stdoutFile + "'", getExecutorParams())
            .getExitCode();
  }

  public static CmdRunnerResult writeFileToStdout(
      CmdRunner cmdRunner, String stdoutFile, LsfExecutorParameters executorParams) {
    // Execute through sh required by LocalRunner to direct output to stdout/err.
    return cmdRunner.execute("sh -c 'cat " + stdoutFile + "'", executorParams);
  }

  public static CmdRunnerResult writeFileToStderr(
      CmdRunner cmdRunner, String stderrFile, LsfExecutorParameters executorParams) {
    // Execute through sh required by LocalRunner to direct output to stdout/err.
    return cmdRunner.execute("sh -c 'cat " + stderrFile + " 1>&2'", executorParams);
  }

  public static String extractBsubJobIdSubmitted(String str) {
    Matcher m = BSUB_JOB_ID_SUBMITTED_PATTERN.matcher(str);
    m.find();
    return m.group(1);
  }

  public static boolean extractBjobsJobIdNotFound(String str) {
    Matcher m = BJOBS_JOB_ID_NOT_FOUND_PATTERN.matcher(str);
    return m.find();
  }

  public static String extractBjobsExitCode(String str) {
    Matcher m = BJOBS_EXIT_CODE_PATTERN.matcher(str);
    m.find();
    return m.group(1);
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

  private static void addArgument(StringBuilder cmd, String argument) {
    cmd.append(" ");
    cmd.append(argument);
  }

  public String getWorkDir(String pipelineName, String processId) {
    if (getExecutorParams().getWorkDir() != null) {
      String workDir = getExecutorParams().getWorkDir();
      workDir = workDir.replace('\\', '/');
      workDir = workDir.trim();
      if (!workDir.endsWith("/")) {
        workDir = workDir + "/";
      }
      return workDir + "pipelite/" + pipelineName + "/" + processId;
    } else {
      return "pipelite/" + pipelineName + "/" + processId;
    }
  }

  public String getOutFile(String pipelineName, String processId, String stageName, String suffix) {
    String workDir = getWorkDir(pipelineName, processId);
    if (!workDir.endsWith("/")) {
      workDir = workDir + "/";
    }
    return workDir + pipelineName + "_" + processId + "_" + stageName + "." + suffix;
  }

  private FluentLogger.Api logContext(
      FluentLogger.Api log, String pipelineName, String processId, Stage stage) {
    return log.with(LogKey.PIPELINE_NAME, pipelineName)
        .with(LogKey.PROCESS_ID, processId)
        .with(LogKey.STAGE_NAME, stage.getStageName());
  }
}
