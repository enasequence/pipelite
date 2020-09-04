package pipelite.executor.lsf;

import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Value;
import lombok.extern.flogger.Flogger;
import pipelite.executor.runner.CommandRunner;
import pipelite.executor.runner.SshRunner;
import pipelite.task.TaskInstance;

@Flogger
@Value
@Builder
@EqualsAndHashCode(callSuper = true)
public final class LsfSshExecutor extends LsfExecutor {

  @Override
  public CommandRunner getCmdRunner() {
    return new SshRunner();
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
