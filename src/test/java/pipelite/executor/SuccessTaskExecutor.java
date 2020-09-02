package pipelite.executor;

import lombok.extern.flogger.Flogger;
import pipelite.task.TaskInstance;
import pipelite.task.TaskExecutionResult;

@Flogger
public class SuccessTaskExecutor implements TaskExecutor {
  @Override
  public TaskExecutionResult execute(TaskInstance taskInstance) {
    return TaskExecutionResult.success();
  }
}
