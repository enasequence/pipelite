package pipelite.executor.describe.context;

import lombok.EqualsAndHashCode;
import lombok.Value;
import lombok.experimental.NonFinal;

@Value
@NonFinal
@EqualsAndHashCode(callSuper = true)
public class AsyncCmdRequestContext extends DefaultRequestContext {
  @EqualsAndHashCode.Exclude private final String outFile;

  public AsyncCmdRequestContext(String jobId, String outFile) {
    super(jobId);
    this.outFile = outFile;
  }

  public String getOutFile() {
    return outFile;
  }
}
