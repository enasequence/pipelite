package pipelite.executor.executable;

import pipelite.executor.TaskExecutor;
import pipelite.instance.TaskInstance;
import pipelite.task.TaskExecutionResult;

public interface ExecutableTaskExecutor extends TaskExecutor {

  interface Resolver {
    TaskExecutionResult resolve(TaskInstance taskInstance, int exitCode);
  }

  TaskExecutionResult resolve(TaskInstance taskInstance, int exitCode);
}
