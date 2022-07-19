package pipelite.executor.describe.context;

import lombok.Value;
import lombok.experimental.NonFinal;

@Value
@NonFinal
public class DefaultRequestContext {
  private final String jobId;

  public DefaultRequestContext(String jobId) {
    this.jobId = jobId;
  }

  public String getJobId() {
    return jobId;
  }
}
