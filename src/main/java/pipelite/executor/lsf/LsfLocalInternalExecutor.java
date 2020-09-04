package pipelite.executor.lsf;

import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Value;
import lombok.extern.flogger.Flogger;
import pipelite.executor.InternalExecutor;
import pipelite.executor.runner.CommandRunner;
import pipelite.executor.runner.LocalRunner;
import pipelite.task.TaskInstance;

@Flogger
@Value
@Builder
@EqualsAndHashCode(callSuper = true)
public final class LsfLocalInternalExecutor extends LsfExecutor {

  @Override
  public CommandRunner getCmdRunner() {
    return new LocalRunner();
  }

  @Override
  public String getCmd(TaskInstance taskInstance) {
    return InternalExecutor.getCmd(taskInstance);
  }
}
