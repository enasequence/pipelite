package pipelite.executor;

import lombok.extern.flogger.Flogger;
import pipelite.executor.TaskExecutor;
import pipelite.executor.runner.CommandRunner;
import pipelite.executor.runner.CommandRunnerResult;
import pipelite.task.TaskExecutionResult;
import pipelite.task.TaskExecutionResultExitCode;
import pipelite.task.TaskInstance;

@Flogger
public abstract class CommandExecutor implements TaskExecutor {

  public abstract CommandRunner getCmdRunner();

  public abstract String getCmd(TaskInstance taskInstance);

  public String getDispatcherCmd(TaskInstance taskInstance) {
    return null;
  }

  public void getDispatcherJobId(TaskExecutionResult taskExecutionResult) {}

  public TaskExecutionResult execute(TaskInstance taskInstance) {
    String cmd = getCmd(taskInstance);
    String dispatchCommand = getDispatcherCmd(taskInstance);
    if (dispatchCommand != null) {
      cmd = dispatchCommand + " " + cmd;
    }

    try {
      CommandRunnerResult commandRunnerResult =
          getCmdRunner().execute(cmd, taskInstance.getTaskParameters());

      TaskExecutionResult result =
          TaskExecutionResultExitCode.resolve(commandRunnerResult.getExitCode());
      result.addAttribute(TaskExecutionResult.COMMAND, cmd);
      result.addAttribute(TaskExecutionResult.HOST, taskInstance.getTaskParameters().getHost());
      result.addAttribute(TaskExecutionResult.EXIT_CODE, commandRunnerResult.getExitCode());
      result.setStdout(commandRunnerResult.getStdout());
      result.setStderr(commandRunnerResult.getStderr());
      return result;

    } catch (Exception ex) {
      log.atSevere().withCause(ex).log("Failed call: %s", cmd);
      TaskExecutionResult result = TaskExecutionResult.error();
      result.addAttribute(TaskExecutionResult.COMMAND, cmd);
      result.addExceptionAttribute(ex);
      getDispatcherJobId(result);
      return result;
    }
  }

  public String getWorkDir(TaskInstance taskInstance) {
    if (taskInstance.getTaskParameters().getWorkDir() != null) {
      return taskInstance.getTaskParameters().getWorkDir();
    } else {
      return "";
    }
  }

  public String getWorkFile(TaskInstance taskInstance, String prefix, String suffix) {
    String workDir = getWorkDir(taskInstance);
    if (!workDir.isEmpty() && !workDir.endsWith("/")) {
      workDir += "/";
    }
    return workDir
        + "pipelite-"
        + prefix
        + "-"
        + taskInstance.getProcessName()
        + "_"
        + taskInstance.getProcessId()
        + "_"
        + taskInstance.getTaskName()
        + "."
        + suffix;
  }
}
