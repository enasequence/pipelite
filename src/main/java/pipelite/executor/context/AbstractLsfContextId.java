package pipelite.executor.context;

import lombok.Builder;
import lombok.Value;

/** Unique combination of fields for operations that can be shared between LSF executors. */
@Value
@Builder
public class AbstractLsfContextId {
  private final String host;
  // TODO: user
}
