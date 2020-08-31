package pipelite.executor;

import lombok.extern.flogger.Flogger;
import pipelite.executor.TaskExecutor;
import pipelite.task.TaskInstance;
import pipelite.task.TaskExecutionResult;

@Flogger
public class PermanentErrorTaskExecutor implements TaskExecutor {
  @Override
  public TaskExecutionResult execute(TaskInstance taskInstance) {
    return TaskExecutionResult.defaultPermanentError();
  }
}
