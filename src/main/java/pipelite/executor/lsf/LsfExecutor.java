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
package pipelite.executor.lsf;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.extern.flogger.Flogger;
import pipelite.executor.CommandExecutor;
import pipelite.executor.PollableExecutor;
import pipelite.executor.runner.CommandRunner;
import pipelite.executor.runner.CommandRunnerResult;
import pipelite.log.LogKey;
import pipelite.task.TaskExecutionResult;
import pipelite.task.TaskExecutionResultExitCode;
import pipelite.task.TaskExecutionResultType;
import pipelite.task.Task;

@Flogger
public abstract class LsfExecutor extends CommandExecutor implements PollableExecutor {

  private String jobId;
  private String stdoutFile;
  private String stderrFile;
  private final LocalDateTime startTime = LocalDateTime.now();

  private static final Pattern JOB_ID_SUBMITTED_PATTERN =
      Pattern.compile("Job <(\\d+)\\> is submitted");
  private static final Pattern JOB_ID_NOT_FOUND_PATTERN =
      Pattern.compile("Job <(\\d+)\\> is not found");
  private static final Pattern EXIT_CODE_PATTERN = Pattern.compile("Exited with exit code (\\d+)");

  @Override
  public final String getDispatcherCmd(Task task) {
    StringBuilder cmd = new StringBuilder();
    cmd.append("bsub");

    stdoutFile = getWorkFile(task, "lsf", "stdout");
    stderrFile = getWorkFile(task, "lsf", "stderr");

    addArgument(cmd, "-oo");
    addArgument(cmd, stdoutFile);
    addArgument(cmd, "-eo");
    addArgument(cmd, stderrFile);

    Integer cores = task.getTaskParameters().getCores();
    if (cores != null && cores > 0) {
      addArgument(cmd, "-n");
      addArgument(cmd, Integer.toString(cores));
    }

    Integer memory = task.getTaskParameters().getMemory();
    Duration memoryTimeout = task.getTaskParameters().getMemoryTimeout();
    if (memory != null && memory > 0) {
      addArgument(cmd, "-M");
      addArgument(cmd, Integer.toString(memory));
      addArgument(cmd, "-R");
      addArgument(
          cmd,
          "rusage[mem="
              + memory
              + ((memoryTimeout == null || memoryTimeout.toMinutes() < 0)
                  ? ""
                  : ":duration=" + memoryTimeout.toMinutes())
              + "]");
    }

    Duration timeout = task.getTaskParameters().getTimeout();
    if (timeout != null) {
      addArgument(cmd, "-W");
      addArgument(cmd, String.valueOf(timeout.toMinutes()));
    }

    String queue = task.getTaskParameters().getQueue();
    if (queue != null) {
      addArgument(cmd, "-q");
      addArgument(cmd, queue);
    }

    return cmd.toString();
  }

  @Override
  public final void getDispatcherJobId(TaskExecutionResult taskExecutionResult) {
    jobId = extractJobIdSubmitted(taskExecutionResult.getStdout());
  }

  @Override
  public final TaskExecutionResult execute(Task task) {

    TaskExecutionResult result = super.execute(task);

    String stdout = result.getStdout();
    String stderr = result.getStderr();
    jobId = extractJobIdSubmitted(stdout);
    if (jobId == null) {
      jobId = extractJobIdSubmitted(stderr);
    }
    if (jobId == null) {
      result.setResultType(TaskExecutionResultType.ERROR);
    } else {
      result.setResultType(TaskExecutionResultType.ACTIVE);
    }
    return result;
  }

  @Override
  public final TaskExecutionResult poll(Task task) {
    Duration timeout = task.getTaskParameters().getTimeout();
    while (true) {
      if (timeout != null && LocalDateTime.now().isAfter(startTime.plus(timeout))) {
        log.atSevere()
            .with(LogKey.PROCESS_NAME, task.getProcessName())
            .with(LogKey.PROCESS_ID, task.getProcessId())
            .with(LogKey.TASK_NAME, task.getTaskName())
            .log("Maximum run time exceeded. Killing LSF job.");

        getCmdRunner().execute("bkill " + jobId, task.getTaskParameters());
        return TaskExecutionResult.error();
      }

      log.atInfo()
          .with(LogKey.PROCESS_NAME, task.getProcessName())
          .with(LogKey.PROCESS_ID, task.getProcessId())
          .with(LogKey.TASK_NAME, task.getTaskName())
          .log("Checking LSF job result using bjobs.");

      CommandRunnerResult bjobsCommandRunnerResult =
          getCmdRunner().execute("bjobs -l " + jobId, task.getTaskParameters());

      TaskExecutionResult result = getResult(bjobsCommandRunnerResult.getStdout());

      if (result == null && extractJobIdNotFound(bjobsCommandRunnerResult.getStdout())) {
        log.atInfo()
            .with(LogKey.PROCESS_NAME, task.getProcessName())
            .with(LogKey.PROCESS_ID, task.getProcessId())
            .with(LogKey.TASK_NAME, task.getTaskName())
            .log("Checking LSF job result using bhist.");

        CommandRunnerResult bhistCommandRunnerResult =
            getCmdRunner().execute("bhist -l " + jobId, task.getTaskParameters());

        result = getResult(bhistCommandRunnerResult.getStdout());
      }

      if (result != null) {
        log.atInfo()
            .with(LogKey.PROCESS_NAME, task.getProcessName())
            .with(LogKey.PROCESS_ID, task.getProcessId())
            .with(LogKey.TASK_NAME, task.getTaskName())
            .log("Reading stdout file: %s", stdoutFile);

        try {
          CommandRunnerResult stdoutCommandRunnerResult =
              writeFileToStdout(getCmdRunner(), stdoutFile, task);
          result.setStdout(stdoutCommandRunnerResult.getStdout());
        } catch (Exception ex) {
          log.atSevere().withCause(ex).log("Failed to read stdout file: %s", stdoutFile);
        }

        log.atInfo()
            .with(LogKey.PROCESS_NAME, task.getProcessName())
            .with(LogKey.PROCESS_ID, task.getProcessId())
            .with(LogKey.TASK_NAME, task.getTaskName())
            .log("Reading stderr file: %s", stderrFile);

        try {
          CommandRunnerResult stderrCommandRunnerResult =
              writeFileToStderr(getCmdRunner(), stderrFile, task);
          result.setStderr(stderrCommandRunnerResult.getStderr());
        } catch (Exception ex) {
          log.atSevere().withCause(ex).log("Failed to read stderr file: %s", stderrFile);
        }

        return result;
      }

      try {
        Thread.sleep(getPollFrequency(task).toMillis());
      } catch (InterruptedException ex) {
        Thread.currentThread().interrupt();
      }
    }
  }

  public static CommandRunnerResult writeFileToStdout(
      CommandRunner cmdRunner, String stdoutFile, Task task) {
    // Execute through sh required by LocalRunner to direct output to stdout/err.
    return cmdRunner.execute("sh -c 'cat " + stdoutFile + "'", task.getTaskParameters());
  }

  public static CommandRunnerResult writeFileToStderr(
      CommandRunner cmdRunner, String stderrFile, Task task) {
    // Execute through sh required by LocalRunner to direct output to stdout/err.
    return cmdRunner.execute(
        "sh -c 'cat " + stderrFile + " 1>&2'", task.getTaskParameters());
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

  private static TaskExecutionResult getResult(String str) {
    if (str.contains("Done successfully")) {
      TaskExecutionResult result = TaskExecutionResult.success();
      result.getAttributes().put(TaskExecutionResult.EXIT_CODE, "0");
      return result;
    }

    if (str.contains("Completed <exit>")) {
      int exitCode = Integer.valueOf(extractExitCode(str));
      TaskExecutionResult result = TaskExecutionResultExitCode.deserialize(exitCode);
      result.getAttributes().put(TaskExecutionResult.EXIT_CODE, String.valueOf(exitCode));
      return result;
    }
    return null;
  }

  private static void addArgument(StringBuilder cmd, String argument) {
    cmd.append(" ");
    cmd.append(argument);
  }
}
