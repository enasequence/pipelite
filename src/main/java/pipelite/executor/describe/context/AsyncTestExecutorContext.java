package pipelite.executor.describe.context;

import lombok.EqualsAndHashCode;
import lombok.Value;
import lombok.experimental.NonFinal;
import pipelite.executor.AsyncTestExecutor;

@Value
@NonFinal
@EqualsAndHashCode(callSuper = true)
public final class AsyncTestExecutorContext
    extends DefaultExecutorContext<AsyncTestRequestContext> {

  public AsyncTestExecutorContext() {
    super("AsyncTest", requests -> AsyncTestExecutor.pollJobs(requests), null);
  }
}
