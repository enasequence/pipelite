package pipelite.executor.call;

import lombok.Value;
import pipelite.executor.TaskExecutor;
import pipelite.task.TaskInstance;
import pipelite.task.TaskExecutionResult;
import pipelite.task.TaskParameters;

public interface CallExecutor extends TaskExecutor {

  @Value
  class CallResult {
    private final int exitCode;
    private final String stdout;
    private final String stderr;
  }

  interface Call {
    CallExecutor.CallResult call(String cmd, TaskParameters taskParameters);
  }

  Call getCall();

  String getCmd(TaskInstance taskInstance);

  default String getWorkDir(TaskInstance taskInstance) {
    if (taskInstance.getTaskParameters().getWorkDir() != null) {
      return taskInstance.getTaskParameters().getWorkDir();
    } else {
      return "";
    }
  }

  default String getWorkFile(TaskInstance taskInstance, String prefix, String suffix) {
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
