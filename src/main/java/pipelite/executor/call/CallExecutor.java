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

  interface Resolver {
    TaskExecutionResult resolve(TaskInstance taskInstance, int exitCode);
  }

  Resolver getResolver();
}
