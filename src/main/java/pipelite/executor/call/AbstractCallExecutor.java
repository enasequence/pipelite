package pipelite.executor.call;

import lombok.extern.flogger.Flogger;
import pipelite.task.TaskExecutionResult;
import pipelite.task.TaskInstance;

@Flogger
public abstract class AbstractCallExecutor implements CallExecutor {

  @Override
  public TaskExecutionResult execute(TaskInstance taskInstance) {
    String cmd = getCmd(taskInstance);

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
      TaskExecutionResult result = TaskExecutionResult.defaultInternalError();
      result.addAttribute(TaskExecutionResult.STANDARD_ATTRIBUTE_COMMAND, cmd);
      result.addExceptionAttribute(ex);
      return result;
    }
  }
}
