package pipelite.executor.call;

import lombok.extern.flogger.Flogger;
import pipelite.task.TaskExecutionResult;
import pipelite.task.TaskInstance;

@Flogger
public abstract class AbstractCallExecutor implements CallExecutor {

  public String getDispatchCmd(TaskInstance taskInstance) {
    return null;
  }

  public void extractDispatchJobId(TaskExecutionResult taskExecutionResult) {}

  @Override
  public TaskExecutionResult execute(TaskInstance taskInstance) {
    String cmd = getCmd(taskInstance);
    String dispatchCommand = getDispatchCmd(taskInstance);
    if (dispatchCommand != null) {
      cmd = dispatchCommand + " " + cmd;
    }

    try {
      CallResult callResult = getCall().call(cmd, taskInstance.getTaskParameters());

      TaskExecutionResult result = getResolver().resolve(taskInstance, callResult.getExitCode());
      result.addAttribute(TaskExecutionResult.STANDARD_ATTRIBUTE_COMMAND, cmd);
      result.addAttribute(
          TaskExecutionResult.STANDARD_ATTRIBUTE_HOST, taskInstance.getTaskParameters().getHost());
      result.addAttribute(
          TaskExecutionResult.STANDARD_ATTRIBUTE_EXIT_CODE, callResult.getExitCode());
      result.addAttribute(TaskExecutionResult.STANDARD_ATTRIBUTE_STDOUT, callResult.getStdout());
      result.addAttribute(TaskExecutionResult.STANDARD_ATTRIBUTE_STDERR, callResult.getStderr());
      return result;

    } catch (Exception ex) {
      log.atSevere().withCause(ex).log("Failed call: %s", cmd);
      TaskExecutionResult result = TaskExecutionResult.error();
      result.addAttribute(TaskExecutionResult.STANDARD_ATTRIBUTE_COMMAND, cmd);
      result.addExceptionAttribute(ex);
      extractDispatchJobId(result);
      return result;
    }
  }
}
