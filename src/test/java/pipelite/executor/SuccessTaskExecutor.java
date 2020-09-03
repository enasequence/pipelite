package pipelite.executor;

import lombok.extern.flogger.Flogger;
import pipelite.task.TaskInstance;
import pipelite.task.TaskExecutionResult;

@Flogger
public class SuccessTaskExecutor implements TaskExecutor, PollableExecutor {
  @Override
  public TaskExecutionResult execute(TaskInstance taskInstance) {
    return TaskExecutionResult.success();
  }

  @Override
  public TaskExecutionResult poll(TaskInstance taskinstance) {
    return TaskExecutionResult.success();
  }
}
