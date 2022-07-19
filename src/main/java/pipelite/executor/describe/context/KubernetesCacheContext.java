package pipelite.executor.describe.context;

import lombok.EqualsAndHashCode;
import lombok.Value;
import lombok.experimental.NonFinal;

@Value
@NonFinal
@EqualsAndHashCode(callSuper = true)
public final class KubernetesCacheContext extends DefaultCacheContext {
  private final String context;
  private final String namespace;

  public KubernetesCacheContext(String context, String namespace) {
    this.context = context;
    this.namespace = namespace;
  }
}
