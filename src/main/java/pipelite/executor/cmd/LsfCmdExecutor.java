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
package pipelite.executor.cmd;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.extern.flogger.Flogger;
import pipelite.exception.PipeliteInterruptedException;
import pipelite.executor.cmd.runner.CmdRunner;
import pipelite.executor.cmd.runner.CmdRunnerResult;
import pipelite.log.LogKey;
import pipelite.stage.*;
import pipelite.time.Time;

@Flogger
@Data
@EqualsAndHashCode(callSuper = true)
public class LsfCmdExecutor extends CmdExecutor {

  private String jobId;
  private String stdoutFile;
  private LocalDateTime startTime;

  private static final String BSUB_CMD = "bsub";
  private static final String BJOBS_STANDARD_CMD = "bjobs -l ";
  private static final String BJOBS_CUSTOM_CMD =
      "bjobs -o \"stat exit_code cpu_used max_mem avg_mem exec_host delimiter='|'\" -noheader ";
  private static final String BHIST_CMD = "bhist -l ";
  private static final String BKILL_CMD = "bkill ";
  private static final String MKDIR_CMD = "mkdir -p ";

  private static final Pattern JOB_ID_SUBMITTED_PATTERN =
      Pattern.compile("Job <(\\d+)\\> is submitted");
  private static final Pattern JOB_ID_NOT_FOUND_PATTERN =
      Pattern.compile("Job <(\\d+)\\> is not found");
  private static final Pattern EXIT_CODE_PATTERN = Pattern.compile("Exited with exit code (\\d+)");

  private static final String STATUS_DONE = "DONE";
  private static final String STATUS_EXIT = "EXIT";
  private static final int BJOBS_STATUS = 0;
  private static final int BJOBS_EXIT_CODE = 1;
  private static final int BJOBS_CPU_TIME = 2;
  private static final int BJOBS_MAX_MEM = 3;
  private static final int BJOBS_AVG_MEM = 4;
  private static final int BJOBS_HOST = 5;

  private static final Duration STDOUT_FILE_MAX_WAIT_TIME = Duration.ofMinutes(5);
  private static final Duration STDOUT_FILE_POLL_WAIT_TIME = Duration.ofSeconds(10);

  @Override
  public StageExecutionResult execute(String pipelineName, String processId, Stage stage) {
    if (jobId == null) {
      return submit(pipelineName, processId, stage);
    }
    return poll(pipelineName, processId, stage);
  }

  private StageExecutionResult submit(String pipelineName, String processId, Stage stage) {

    startTime = LocalDateTime.now();

    if (!getWorkDir(pipelineName, processId, stage).isEmpty()) {
      CmdRunner cmdRunner = getCmdRunner();
      cmdRunner.execute(
          MKDIR_CMD + getWorkDir(pipelineName, processId, stage), stage.getExecutorParams());
    }

    StageExecutionResult result = super.execute(pipelineName, processId, stage);

    String stdout = result.getStdout();
    String stderr = result.getStderr();
    jobId = extractJobIdSubmitted(stdout);
    if (jobId == null) {
      jobId = extractJobIdSubmitted(stderr);
    }
    if (jobId == null) {
      result.setResultType(StageExecutionResultType.ERROR);
    } else {
      result.setResultType(StageExecutionResultType.ACTIVE);
    }
    return result;
  }

  private StageExecutionResult poll(String pipelineName, String processId, Stage stage) {

    CmdRunner cmdRunner = getCmdRunner();

    Duration timeout = stage.getExecutorParams().getTimeout();

    if (timeout != null && LocalDateTime.now().isAfter(startTime.plus(timeout))) {
      log.atSevere()
          .with(LogKey.PIPELINE_NAME, pipelineName)
          .with(LogKey.PROCESS_ID, processId)
          .with(LogKey.STAGE_NAME, stage.getStageName())
          .log("Maximum run time exceeded. Killing LSF job.");

      cmdRunner.execute(BKILL_CMD + jobId, stage.getExecutorParams());
      reset();
      return StageExecutionResult.error();
    }

    log.atFine()
        .with(LogKey.PIPELINE_NAME, pipelineName)
        .with(LogKey.PROCESS_ID, processId)
        .with(LogKey.STAGE_NAME, stage.getStageName())
        .log("Checking LSF job result using bjobs.");

    CmdRunnerResult bjobsCustomCmdRunnerResult =
        cmdRunner.execute(BJOBS_CUSTOM_CMD + jobId, stage.getExecutorParams());

    StageExecutionResult result;

    if (!extractJobIdNotFound(bjobsCustomCmdRunnerResult.getStdout())) {
      result = getCustomJobResult(bjobsCustomCmdRunnerResult.getStdout());
      if (result.isActive()) {
        return result;
      }
    } else {
      log.atFine()
          .with(LogKey.PIPELINE_NAME, pipelineName)
          .with(LogKey.PROCESS_ID, processId)
          .with(LogKey.STAGE_NAME, stage.getStageName())
          .log("Checking LSF job result using bhist.");

      CmdRunnerResult bhistCmdRunnerResult =
          cmdRunner.execute(BHIST_CMD + jobId, stage.getExecutorParams());

      result = getStandardJobResult(bhistCmdRunnerResult.getStdout());
      if (result == null) {
        reset();
        result = StageExecutionResult.error();
      }
    }

    // Check if the stdout file exists. The file may not be immediately available after the job
    // execution finishes.

    LocalDateTime stdoutFileWaitStart = LocalDateTime.now();
    while (stdoutFileWaitStart.plus(STDOUT_FILE_MAX_WAIT_TIME).isAfter(LocalDateTime.now())
        && !stdoutFileExists(getCmdRunner(), stdoutFile, stage)) {

      if (!Time.wait(STDOUT_FILE_POLL_WAIT_TIME)) {
        throw new PipeliteInterruptedException("LSF command executor was interrupted");
      }
    }

    log.atFine()
        .with(LogKey.PIPELINE_NAME, pipelineName)
        .with(LogKey.PROCESS_ID, processId)
        .with(LogKey.STAGE_NAME, stage.getStageName())
        .log("Reading stdout file: %s", stdoutFile);

    try {
      CmdRunnerResult stdoutCmdRunnerResult = writeFileToStdout(getCmdRunner(), stdoutFile, stage);
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
  public final String getDispatcherCmd(String pipelineName, String processId, Stage stage) {

    StringBuilder cmd = new StringBuilder();
    cmd.append(BSUB_CMD);

    stdoutFile = getOutFile(pipelineName, processId, stage, "stdout");

    // Write both stderr and stdout to the stdout file.

    addArgument(cmd, "-oo");
    addArgument(cmd, stdoutFile);

    Integer cores = stage.getExecutorParams().getCores();
    if (cores != null && cores > 0) {
      addArgument(cmd, "-n");
      addArgument(cmd, Integer.toString(cores));
    }

    Integer memory = stage.getExecutorParams().getMemory();
    Duration memoryTimeout = stage.getExecutorParams().getMemoryTimeout();
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

    Duration timeout = stage.getExecutorParams().getTimeout();
    if (timeout != null) {
      if (timeout.toMinutes() == 0) {
        timeout = Duration.ofMinutes(1);
      }
      addArgument(cmd, "-W");
      addArgument(cmd, String.valueOf(timeout.toMinutes()));
    }

    String queue = stage.getExecutorParams().getQueue();
    if (queue != null) {
      addArgument(cmd, "-q");
      addArgument(cmd, queue);
    }

    return cmd.toString();
  }

  @Override
  public final void getDispatcherJobId(StageExecutionResult stageExecutionResult) {
    jobId = extractJobIdSubmitted(stageExecutionResult.getStdout());
  }

  public static boolean stdoutFileExists(CmdRunner cmdRunner, String stdoutFile, Stage stage) {
    // Check if the stdout file exists. The file may not be immediately available after the job
    // execution finishes.
    return 0
        == cmdRunner
            .execute("sh -c 'test -f " + stdoutFile + "'", stage.getExecutorParams())
            .getExitCode();
  }

  public static CmdRunnerResult writeFileToStdout(
      CmdRunner cmdRunner, String stdoutFile, Stage stage) {
    // Execute through sh required by LocalRunner to direct output to stdout/err.
    return cmdRunner.execute("sh -c 'cat " + stdoutFile + "'", stage.getExecutorParams());
  }

  public static CmdRunnerResult writeFileToStderr(
      CmdRunner cmdRunner, String stderrFile, Stage stage) {
    // Execute through sh required by LocalRunner to direct output to stdout/err.
    return cmdRunner.execute("sh -c 'cat " + stderrFile + " 1>&2'", stage.getExecutorParams());
  }

  public static String extractJobIdSubmitted(String str) {
    Matcher m = JOB_ID_SUBMITTED_PATTERN.matcher(str);
    m.find();
    return m.group(1);
  }

  public static boolean extractJobIdNotFound(String str) {
    Matcher m = JOB_ID_NOT_FOUND_PATTERN.matcher(str);
    return m.find();
  }

  public static String extractExitCode(String str) {
    Matcher m = EXIT_CODE_PATTERN.matcher(str);
    m.find();
    return m.group(1);
  }

  private StageExecutionResult getCustomJobResult(String str) {
    String[] bjobs = str.split("\\|");

    int exitCode = 0;
    StageExecutionResult result = null;

    if (bjobs[BJOBS_STATUS].equals(STATUS_DONE)) {
      result = StageExecutionResult.success();
    } else if (bjobs[BJOBS_STATUS].equals(STATUS_EXIT)) {
      exitCode = Integer.valueOf(bjobs[BJOBS_EXIT_CODE]);
      result = StageExecutionResult.error();
    }

    if (result != null) {
      result.getAttributes().put(StageExecutionResult.EXIT_CODE, String.valueOf(exitCode));
      result.getAttributes().put(StageExecutionResult.EXEC_HOST, bjobs[BJOBS_HOST]);
      result.getAttributes().put(StageExecutionResult.CPU_TIME, bjobs[BJOBS_CPU_TIME]);
      result.getAttributes().put(StageExecutionResult.MAX_MEM, bjobs[BJOBS_MAX_MEM]);
      result.getAttributes().put(StageExecutionResult.AVG_MEM, bjobs[BJOBS_AVG_MEM]);
      return result;
    }

    return StageExecutionResult.active();
  }

  private static StageExecutionResult getStandardJobResult(String str) {
    if (str.contains("Done successfully")) {
      StageExecutionResult result = StageExecutionResult.success();
      result.getAttributes().put(StageExecutionResult.EXIT_CODE, "0");
      return result;
    }

    if (str.contains("Completed <exit>")) {
      int exitCode = Integer.valueOf(extractExitCode(str));
      StageExecutionResult result = StageExecutionResult.error();
      result.getAttributes().put(StageExecutionResult.EXIT_CODE, String.valueOf(exitCode));
      return result;
    }

    return null;
  }

  private static void addArgument(StringBuilder cmd, String argument) {
    cmd.append(" ");
    cmd.append(argument);
  }
}
