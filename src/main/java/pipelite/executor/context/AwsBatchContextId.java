package pipelite.executor.context;

import lombok.Builder;
import lombok.Value;

/** Unique combination of fields for operations that can be shared between AWSBatch executors. */
@Value
@Builder
public class AwsBatchContextId {
  private final String region;
  // TODO: user
}
