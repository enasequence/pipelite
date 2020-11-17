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

import lombok.extern.flogger.Flogger;
import pipelite.executor.PollableExecutor;
import pipelite.executor.cmd.runner.CmdRunner;
import pipelite.executor.cmd.runner.CmdRunnerResult;
import pipelite.log.LogKey;
import pipelite.stage.Stage;
import pipelite.stage.StageExecutionResult;
import pipelite.stage.StageExecutionResultExitCode;
import pipelite.stage.StageExecutionResultType;

@Flogger
public class LsfCmdExecutor extends CmdExecutor implements PollableExecutor {

  private String jobId;
  private String stdoutFile;
  private String stderrFile;
  private LocalDateTime startTime = LocalDateTime.now();

  private static final Pattern JOB_ID_SUBMITTED_PATTERN =
      Pattern.compile("Job <(\\d+)\\> is submitted");
  private static final Pattern JOB_ID_NOT_FOUND_PATTERN =
      Pattern.compile("Job <(\\d+)\\> is not found");
  private static final Pattern EXIT_CODE_PATTERN = Pattern.compile("Exited with exit code (\\d+)");

  @Override
  public final String getDispatcherCmd(Stage stage) {
    StringBuilder cmd = new StringBuilder();
    cmd.append("bsub");

    stdoutFile = getWorkFile(stage, "lsf", "stdout");
    stderrFile = getWorkFile(stage, "lsf", "stderr");

    addArgument(cmd, "-oo");
    addArgument(cmd, stdoutFile);
    addArgument(cmd, "-eo");
    addArgument(cmd, stderrFile);

    Integer cores = stage.getStageParameters().getCores();
    if (cores != null && cores > 0) {
      addArgument(cmd, "-n");
      addArgument(cmd, Integer.toString(cores));
    }

    Integer memory = stage.getStageParameters().getMemory();
    Duration memoryTimeout = stage.getStageParameters().getMemoryTimeout();
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

    Duration timeout = stage.getStageParameters().getTimeout();
    if (timeout != null) {
      if (timeout.toMinutes() == 0){
        timeout = Duration.ofMinutes(1);
      }
      addArgument(cmd, "-W");
      addArgument(cmd, String.valueOf(timeout.toMinutes()));
    }

    String queue = stage.getStageParameters().getQueue();
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

  @Override
  public final StageExecutionResult execute(Stage stage) {

    StageExecutionResult result = super.execute(stage);

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

  @Override
  public final StageExecutionResult poll(Stage stage) {
    CmdRunner cmdRunner = getCmdRunner();

    Duration timeout = stage.getStageParameters().getTimeout();
    while (true) {
      if (timeout != null && LocalDateTime.now().isAfter(startTime.plus(timeout))) {
        log.atSevere()
            .with(LogKey.PIPELINE_NAME, stage.getPipelineName())
            .with(LogKey.PROCESS_ID, stage.getProcessId())
            .with(LogKey.STAGE_NAME, stage.getStageName())
            .log("Maximum run time exceeded. Killing LSF job.");

        cmdRunner.execute("bkill " + jobId, stage.getStageParameters());
        return StageExecutionResult.error();
      }

      log.atInfo()
          .with(LogKey.PIPELINE_NAME, stage.getPipelineName())
          .with(LogKey.PROCESS_ID, stage.getProcessId())
          .with(LogKey.STAGE_NAME, stage.getStageName())
          .log("Checking LSF job result using bjobs.");

      CmdRunnerResult bjobsCmdRunnerResult =
          cmdRunner.execute("bjobs -l " + jobId, stage.getStageParameters());

      StageExecutionResult result = getResult(bjobsCmdRunnerResult.getStdout());

      if (result == null && extractJobIdNotFound(bjobsCmdRunnerResult.getStdout())) {
        log.atInfo()
            .with(LogKey.PIPELINE_NAME, stage.getPipelineName())
            .with(LogKey.PROCESS_ID, stage.getProcessId())
            .with(LogKey.STAGE_NAME, stage.getStageName())
            .log("Checking LSF job result using bhist.");

        CmdRunnerResult bhistCmdRunnerResult =
            cmdRunner.execute("bhist -l " + jobId, stage.getStageParameters());

        result = getResult(bhistCmdRunnerResult.getStdout());
      }

      if (result != null) {
        log.atInfo()
            .with(LogKey.PIPELINE_NAME, stage.getPipelineName())
            .with(LogKey.PROCESS_ID, stage.getProcessId())
            .with(LogKey.STAGE_NAME, stage.getStageName())
            .log("Reading stdout file: %s", stdoutFile);

        try {
          CmdRunnerResult stdoutCmdRunnerResult =
              writeFileToStdout(getCmdRunner(), stdoutFile, stage);
          result.setStdout(stdoutCmdRunnerResult.getStdout());
        } catch (Exception ex) {
          log.atSevere().withCause(ex).log("Failed to read stdout file: %s", stdoutFile);
        }

        log.atInfo()
            .with(LogKey.PIPELINE_NAME, stage.getPipelineName())
            .with(LogKey.PROCESS_ID, stage.getProcessId())
            .with(LogKey.STAGE_NAME, stage.getStageName())
            .log("Reading stderr file: %s", stderrFile);

        try {
          CmdRunnerResult stderrCmdRunnerResult =
              writeFileToStderr(getCmdRunner(), stderrFile, stage);
          result.setStderr(stderrCmdRunnerResult.getStderr());
        } catch (Exception ex) {
          log.atSevere().withCause(ex).log("Failed to read stderr file: %s", stderrFile);
        }

        return result;
      }

      try {
        Thread.sleep(getPollFrequency(stage).toMillis());
      } catch (InterruptedException ex) {
        Thread.currentThread().interrupt();
      }
    }
  }

  public static CmdRunnerResult writeFileToStdout(
      CmdRunner cmdRunner, String stdoutFile, Stage stage) {
    // Execute through sh required by LocalRunner to direct output to stdout/err.
    return cmdRunner.execute("sh -c 'cat " + stdoutFile + "'", stage.getStageParameters());
  }

  public static CmdRunnerResult writeFileToStderr(
      CmdRunner cmdRunner, String stderrFile, Stage stage) {
    // Execute through sh required by LocalRunner to direct output to stdout/err.
    return cmdRunner.execute("sh -c 'cat " + stderrFile + " 1>&2'", stage.getStageParameters());
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

  private static StageExecutionResult getResult(String str) {
    if (str.contains("Done successfully")) {
      StageExecutionResult result = StageExecutionResult.success();
      result.getAttributes().put(StageExecutionResult.EXIT_CODE, "0");
      return result;
    }

    if (str.contains("Completed <exit>")) {
      int exitCode = Integer.valueOf(extractExitCode(str));
      StageExecutionResult result = StageExecutionResultExitCode.deserialize(exitCode);
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
