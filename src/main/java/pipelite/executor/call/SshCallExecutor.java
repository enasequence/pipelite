package pipelite.executor.call;

import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Value;
import lombok.extern.flogger.Flogger;
import pipelite.task.TaskInstance;

@Flogger
@Value
@Builder
@EqualsAndHashCode(callSuper=true)
public final class SshCallExecutor extends AbstractSshCallExecutor {
  private final Cmd cmd;

  public interface Cmd {
    String getCmd(TaskInstance taskInstance);
  }

  @Override
  public String getCmd(TaskInstance taskInstance) {
    return cmd.getCmd(taskInstance);
  }
}
