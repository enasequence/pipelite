package pipelite.executor.call;

import lombok.extern.flogger.Flogger;
import pipelite.executor.InternalExecutor;
import pipelite.task.TaskInstance;

@Flogger
public final class SshCallInternalExecutor extends AbstractSshCallExecutor {

  @Override
  public String getCmd(TaskInstance taskInstance) {
    return InternalExecutor.getCmd(taskInstance);
  }
}
