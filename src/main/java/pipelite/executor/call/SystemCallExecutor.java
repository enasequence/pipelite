package pipelite.executor.call;

import lombok.Builder;
import lombok.Value;
import lombok.extern.flogger.Flogger;
import pipelite.task.TaskInstance;

@Flogger
@Value
@Builder
public final class SystemCallExecutor extends AbstractSystemCallExecutor {
  private final Cmd cmd;

  public interface Cmd {
    String getCmd(TaskInstance taskInstance);
  }

  @Override
  public String getCmd(TaskInstance taskInstance) {
    return cmd.getCmd(taskInstance);
  }
}
