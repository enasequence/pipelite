package pipelite.executor.describe.context;

import com.amazonaws.services.batch.AWSBatch;
import lombok.EqualsAndHashCode;
import lombok.Value;
import lombok.experimental.NonFinal;
import pipelite.executor.AwsBatchExecutor;

@Value
@NonFinal
@EqualsAndHashCode(callSuper = true)
public final class AwsBatchExecutorContext extends DefaultExecutorContext<DefaultRequestContext> {

  public AwsBatchExecutorContext(AWSBatch awsBatch) {
    super("AWSBatch", requests -> AwsBatchExecutor.pollJobs(awsBatch, requests), null);
  }
}
