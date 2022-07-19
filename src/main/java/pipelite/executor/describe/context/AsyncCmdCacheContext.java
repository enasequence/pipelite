package pipelite.executor.describe.context;

import lombok.EqualsAndHashCode;
import lombok.Value;
import lombok.experimental.NonFinal;

@Value
@NonFinal
@EqualsAndHashCode(callSuper = true)
public class AsyncCmdCacheContext extends DefaultCacheContext {
  private final String host;

  public AsyncCmdCacheContext(String host) {
    this.host = host;
  }
}
