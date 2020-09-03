package pipelite.executor.call;

import lombok.extern.flogger.Flogger;
import pipelite.executor.InternalExecutor;
import pipelite.task.TaskInstance;

@Flogger
public final class SystemCallInternalExecutor extends AbstractSystemCallExecutor {

  @Override
  public String getCmd(TaskInstance taskInstance) {
    return InternalExecutor.getCmd(taskInstance);
  }
}
