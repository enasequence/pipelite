package pipelite.executor.lsf;

import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Value;
import lombok.extern.flogger.Flogger;
import pipelite.executor.InternalExecutor;
import pipelite.executor.runner.CommandRunner;
import pipelite.executor.runner.SshRunner;
import pipelite.task.TaskInstance;

@Flogger
@Value
@Builder
@EqualsAndHashCode(callSuper = true)
public final class LsfSshInternalExecutor extends LsfExecutor {

  @Override
  public CommandRunner getCmdRunner() {
    return new SshRunner();
  }

  @Override
  public String getCmd(TaskInstance taskInstance) {
    return InternalExecutor.getCmd(taskInstance);
  }
}
