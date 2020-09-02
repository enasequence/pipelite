package pipelite.executor.lsf;

import lombok.Builder;
import lombok.Value;
import lombok.extern.flogger.Flogger;
import pipelite.executor.call.AbstractSshCallExecutor;
import pipelite.executor.call.SshCall;
import pipelite.resolver.DefaultExitCodeResolver;
import pipelite.task.TaskExecutionResult;
import pipelite.task.TaskInstance;

@Flogger
@Value
@Builder
public final class SshCallLsfExecutor extends AbstractLsfExecutor {
  private final Cmd cmd;

  public interface Cmd {
    String getCmd(TaskInstance taskInstance);
  }

  @Override
  public String getCmd(TaskInstance taskInstance) {
    return cmd.getCmd(taskInstance);
  }

  @Override
  public Call getCall() {
    return new SshCall();
  }

  @Override
  public Resolver getResolver() {
    return (taskInstance, exitCode) -> (new DefaultExitCodeResolver()).resolve(exitCode);
  }
}
