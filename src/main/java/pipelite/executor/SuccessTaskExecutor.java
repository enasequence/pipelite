package pipelite.executor;

import lombok.extern.flogger.Flogger;
import org.apache.commons.exec.*;
import org.apache.commons.exec.util.StringUtils;
import pipelite.instance.TaskInstance;
import pipelite.task.TaskExecutionResult;

import java.util.Collection;

@Flogger
public class SuccessTaskExecutor implements TaskExecutor {
  @Override
  public TaskExecutionResult execute(TaskInstance taskInstance) {
    return TaskExecutionResult.success();
  }
}
