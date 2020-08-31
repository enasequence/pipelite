package pipelite.executor.executable.ssh;

import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.NonNull;
import lombok.Value;
import pipelite.executor.executable.ssh.AbstractSshExecutor;
import pipelite.task.TaskInstance;
import pipelite.task.TaskExecutionResult;

@Value
@Builder
@EqualsAndHashCode(callSuper=true)
public class SshExecutor extends AbstractSshExecutor {

  @NonNull private final Command command;

  public interface Command {
    String getCommand(TaskInstance taskInstance);
  }

  @Override
  public String getCommand(TaskInstance taskInstance) {
    return command.getCommand(taskInstance);
  }

  @Override
  public TaskExecutionResult resolve(TaskInstance taskInstance, int exitCode) {
    return taskInstance.getResolver().resolve(exitCode);
  }
}
