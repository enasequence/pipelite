package pipelite.executor.describe.context;

import lombok.EqualsAndHashCode;
import lombok.Value;
import lombok.experimental.NonFinal;

@Value
@NonFinal
@EqualsAndHashCode(callSuper = true)
public final class AwsBatchCacheContext extends DefaultCacheContext {
  private final String region;

  public AwsBatchCacheContext(String region) {
    this.region = region;
  }
}
