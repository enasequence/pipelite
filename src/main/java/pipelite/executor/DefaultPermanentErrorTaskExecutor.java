package pipelite.executor;

import lombok.extern.flogger.Flogger;
import pipelite.instance.TaskInstance;
import pipelite.task.TaskExecutionResult;

@Flogger
public class DefaultPermanentErrorTaskExecutor implements TaskExecutor {
  @Override
  public TaskExecutionResult execute(TaskInstance taskInstance) {
    return TaskExecutionResult.defaultPermanentError();
  }
}
