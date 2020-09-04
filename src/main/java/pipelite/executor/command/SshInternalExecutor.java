package pipelite.executor.command;

import lombok.extern.flogger.Flogger;
import pipelite.executor.InternalExecutor;
import pipelite.executor.CommandExecutor;
import pipelite.executor.runner.CommandRunner;
import pipelite.executor.runner.SshRunner;
import pipelite.task.TaskInstance;

@Flogger
public final class SshInternalExecutor extends CommandExecutor {

  @Override
  public final CommandRunner getCmdRunner() {
    return new SshRunner();
  }

  @Override
  public String getCmd(TaskInstance taskInstance) {
    return InternalExecutor.getCmd(taskInstance);
  }
}
