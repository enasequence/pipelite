package pipelite.executor.command;

import lombok.extern.flogger.Flogger;
import pipelite.executor.InternalExecutor;
import pipelite.executor.CommandExecutor;
import pipelite.executor.runner.CommandRunner;
import pipelite.executor.runner.LocalRunner;
import pipelite.task.TaskInstance;

@Flogger
public final class LocalInternalExecutor extends CommandExecutor {

  @Override
  public final CommandRunner getCmdRunner() {
    return new LocalRunner();
  }

  @Override
  public String getCmd(TaskInstance taskInstance) {
    return InternalExecutor.getCmd(taskInstance);
  }
}
