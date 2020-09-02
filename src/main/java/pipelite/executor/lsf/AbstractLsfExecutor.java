package pipelite.executor.lsf;

import lombok.extern.flogger.Flogger;
import pipelite.executor.PollableTaskExecutor;
import pipelite.executor.call.AbstractCallExecutor;
import pipelite.log.LogKey;
import pipelite.task.TaskExecutionResult;
import pipelite.task.TaskExecutionResultType;
import pipelite.task.TaskInstance;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Flogger
public abstract class AbstractLsfExecutor extends AbstractCallExecutor
    implements PollableTaskExecutor {

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
  public String getDispatchCmd(TaskInstance taskInstance) {
    StringBuilder cmd = new StringBuilder();
    cmd.append("bsub");

    stdoutFile = getWorkFile(taskInstance, "lsf", "stdout");
    stderrFile = getWorkFile(taskInstance, "lsf", "stderr");

    addArgument(cmd, "-oo");
    addArgument(cmd, stdoutFile);
    addArgument(cmd, "-eo");
    addArgument(cmd, stderrFile);

    Integer cores = taskInstance.getTaskParameters().getCores();
    if (cores != null && cores > 0) {
      addArgument(cmd, "-n");
      addArgument(cmd, Integer.toString(cores));
    }

    Integer memory = taskInstance.getTaskParameters().getMemory();
    Duration memoryTimeout = taskInstance.getTaskParameters().getMemoryTimeout();
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

    Duration timeout = taskInstance.getTaskParameters().getTimeout();
    if (timeout != null) {
      addArgument(cmd, "-W");
      addArgument(cmd, String.valueOf(timeout.toMinutes()));
    }

    String queue = taskInstance.getTaskParameters().getQueue();
    if (queue != null) {
      addArgument(cmd, "-q");
      addArgument(cmd, queue);
    }

    return cmd.toString();
  }

  @Override
  public void extractDispatchJobId(TaskExecutionResult taskExecutionResult) {
    jobId =
        extractJobIdSubmitted(
            taskExecutionResult.getAttribute(TaskExecutionResult.STANDARD_ATTRIBUTE_STDOUT));
  }

  @Override
  public TaskExecutionResult execute(TaskInstance taskInstance) {

    TaskExecutionResult result = super.execute(taskInstance);

    String stdout = result.getAttributes().get(TaskExecutionResult.STANDARD_ATTRIBUTE_STDOUT);
    String stderr = result.getAttributes().get(TaskExecutionResult.STANDARD_ATTRIBUTE_STDERR);
    jobId = extractJobIdSubmitted(stdout);
    if (jobId == null) {
      jobId = extractJobIdSubmitted(stderr);
    }
    if (jobId == null) {
      result.setResultType(TaskExecutionResultType.INTERNAL_ERROR);
    } else {
      result.setResultType(TaskExecutionResultType.ACTIVE);
    }
    return result;
  }

  @Override
  public TaskExecutionResult poll(TaskInstance taskinstance) {

    while (true) {
      Duration timeout = taskinstance.getTaskParameters().getTimeout();
      if (timeout != null && LocalDateTime.now().isAfter(startTime.plus(timeout))) {
        log.atSevere()
            .with(LogKey.PROCESS_NAME, taskinstance.getProcessName())
            .with(LogKey.PROCESS_ID, taskinstance.getProcessId())
            .with(LogKey.TASK_NAME, taskinstance.getTaskName())
            .log("Maximum run time exceeded. Killing LSF job.");

        getCall().call("bkill " + jobId, taskinstance.getTaskParameters());
        return TaskExecutionResult.transientError();
      }

      log.atInfo()
          .with(LogKey.PROCESS_NAME, taskinstance.getProcessName())
          .with(LogKey.PROCESS_ID, taskinstance.getProcessId())
          .with(LogKey.TASK_NAME, taskinstance.getTaskName())
          .log("Checking LSF job result using bjobs.");

      CallResult bjobsCallResult =
          getCall().call("bjobs -l " + jobId, taskinstance.getTaskParameters());

      TaskExecutionResult result = getResult(taskinstance, bjobsCallResult.getStdout());

      if (result == null && extractJobIdNotFound(bjobsCallResult.getStdout())) {
        log.atInfo()
            .with(LogKey.PROCESS_NAME, taskinstance.getProcessName())
            .with(LogKey.PROCESS_ID, taskinstance.getProcessId())
            .with(LogKey.TASK_NAME, taskinstance.getTaskName())
            .log("Checking LSF job result using bhist.");

        CallResult bhistCallResult =
            getCall().call("bhist -l " + jobId, taskinstance.getTaskParameters());

        result = getResult(taskinstance, bhistCallResult.getStdout());
      }

      if (result != null) {
        log.atInfo()
            .with(LogKey.PROCESS_NAME, taskinstance.getProcessName())
            .with(LogKey.PROCESS_ID, taskinstance.getProcessId())
            .with(LogKey.TASK_NAME, taskinstance.getTaskName())
            .log("Reading stdout file: %s", stdoutFile);

        try {
          CallResult stdoutCallResult =
              getCall().call("cat " + stdoutFile, taskinstance.getTaskParameters());
          result.addAttribute(
              TaskExecutionResult.STANDARD_ATTRIBUTE_STDOUT, stdoutCallResult.getStdout());
        } catch (Exception ex) {
          log.atSevere().withCause(ex).log("Failed to read stdout file: %s", stdoutFile);
        }

        log.atInfo()
            .with(LogKey.PROCESS_NAME, taskinstance.getProcessName())
            .with(LogKey.PROCESS_ID, taskinstance.getProcessId())
            .with(LogKey.TASK_NAME, taskinstance.getTaskName())
            .log("Reading stderr file: %s", stderrFile);

        try {
          CallResult stderrCallResult =
              getCall().call("cat " + stderrFile + " 1>&2", taskinstance.getTaskParameters());
          result.addAttribute(
              TaskExecutionResult.STANDARD_ATTRIBUTE_STDERR, stderrCallResult.getStderr());
        } catch (Exception ex) {
          log.atSevere().withCause(ex).log("Failed to read stderr file: %s", stderrFile);
        }

        return result;
      }

      try {
        Thread.sleep(getPollFrequency(taskinstance).toMillis());
      } catch (InterruptedException ex) {
        Thread.currentThread().interrupt();
      }
    }
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

  private static TaskExecutionResult getResult(TaskInstance taskInstance, String str) {
    if (str.contains("Done successfully")) {
      TaskExecutionResult result = TaskExecutionResult.success();
      result.getAttributes().put(TaskExecutionResult.STANDARD_ATTRIBUTE_EXIT_CODE, "0");
      return result;
    }

    if (str.contains("Completed <exit>")) {
      int exitCode = Integer.valueOf(extractExitCode(str));
      TaskExecutionResult result = taskInstance.getResolver().serializer().deserialize(exitCode);
      result
          .getAttributes()
          .put(TaskExecutionResult.STANDARD_ATTRIBUTE_EXIT_CODE, String.valueOf(exitCode));
      return result;
    }
    return null;
  }

  private static void addArgument(StringBuilder cmd, String argument) {
    cmd.append(" ");
    cmd.append(argument);
  }
}
