package pipelite.executor.lsf;

import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Value;
import lombok.extern.flogger.Flogger;
import pipelite.executor.runner.CommandRunner;
import pipelite.executor.runner.LocalRunner;
import pipelite.task.TaskInstance;

@Flogger
@Value
@Builder
@EqualsAndHashCode(callSuper = true)
public final class LsfLocalExecutor extends LsfExecutor {

  @Override
  public CommandRunner getCmdRunner() {
    return new LocalRunner();
  }

  private final Cmd cmd;

  public interface Cmd {
    String getCmd(TaskInstance taskInstance);
  }

  @Override
  public String getCmd(TaskInstance taskInstance) {
    return cmd.getCmd(taskInstance);
  }
}
