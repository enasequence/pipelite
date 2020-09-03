package pipelite.executor.call;

import lombok.extern.flogger.Flogger;

@Flogger
public abstract class AbstractSshCallExecutor extends AbstractCallExecutor {

  @Override
  public final Call getCall() {
    return new SshCall();
  }
}
