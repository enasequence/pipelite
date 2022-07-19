package pipelite.executor.describe.context;

import lombok.EqualsAndHashCode;
import lombok.Value;
import lombok.experimental.NonFinal;

@Value
@NonFinal
@EqualsAndHashCode(callSuper = true)
public class LsfRequestContext extends AsyncCmdRequestContext {
  public LsfRequestContext(String jobId, String outFile) {
    super(jobId, outFile);
  }
}
