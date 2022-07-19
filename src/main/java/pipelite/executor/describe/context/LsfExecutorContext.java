package pipelite.executor.describe.context;

import lombok.EqualsAndHashCode;
import lombok.Value;
import lombok.experimental.NonFinal;
import pipelite.executor.AbstractLsfExecutor;
import pipelite.executor.cmd.CmdRunner;

@Value
@NonFinal
@EqualsAndHashCode(callSuper = true)
public class LsfExecutorContext extends AsyncCmdExecutorContext<LsfRequestContext> {

  public LsfExecutorContext(CmdRunner cmdRunner) {
    super(
        "LSF",
        requests -> AbstractLsfExecutor.pollJobs(cmdRunner, requests),
        request -> AbstractLsfExecutor.recoverJob(cmdRunner, request),
        cmdRunner);
  }
}
