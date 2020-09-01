package pipelite.executor.call;

import lombok.extern.flogger.Flogger;

@Flogger
public abstract class AbstractSshCallExecutor extends AbstractCallExecutor {

  @Override
  public final Call getCall() {
    return new SshCall();
  }

  @Override
  public Resolver getResolver() {
    return (taskInstance, exitCode) -> taskInstance.getResolver().resolve(exitCode);
  }
}
