package pipelite.executor.call;

import lombok.extern.flogger.Flogger;

@Flogger
public abstract class AbstractSystemCallExecutor extends AbstractCallExecutor {

  @Override
  public final Call getCall() {
    return new SystemCall();
  }
}
