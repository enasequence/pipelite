package pipelite.executor.describe.context;

import lombok.EqualsAndHashCode;
import lombok.Value;
import lombok.experimental.NonFinal;
import pipelite.executor.cmd.CmdRunner;

@Value
@NonFinal
@EqualsAndHashCode(callSuper = true)
public class AsyncCmdExecutorContext<RequestContext extends AsyncCmdRequestContext>
    extends DefaultExecutorContext<RequestContext> {
  private final CmdRunner cmdRunner;

  public AsyncCmdExecutorContext(
      String executorName,
      PollJobsCallback<RequestContext> pollJobsCallback,
      RecoverJobCallback<RequestContext> recoverJobCallback,
      CmdRunner cmdRunner) {
    super(executorName, pollJobsCallback, recoverJobCallback);
    this.cmdRunner = cmdRunner;
  }
}
