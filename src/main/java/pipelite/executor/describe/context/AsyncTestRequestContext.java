package pipelite.executor.describe.context;

import lombok.EqualsAndHashCode;
import lombok.Value;
import lombok.experimental.NonFinal;
import pipelite.stage.executor.StageExecutorRequest;
import pipelite.stage.executor.StageExecutorResult;

import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.function.Function;

@Value
@NonFinal
@EqualsAndHashCode(callSuper = true)
public class AsyncTestRequestContext extends DefaultRequestContext {

  @EqualsAndHashCode.Exclude private final StageExecutorRequest request;
  @EqualsAndHashCode.Exclude private final ZonedDateTime startTime;
  @EqualsAndHashCode.Exclude private final Duration executionTime;

  @EqualsAndHashCode.Exclude
  private final Function<StageExecutorRequest, StageExecutorResult> callback;

  public AsyncTestRequestContext(
      String jobId,
      StageExecutorRequest request,
      ZonedDateTime startTime,
      Duration executionTime,
      Function<StageExecutorRequest, StageExecutorResult> callback) {
    super(jobId);
    this.request = request;
    this.startTime = startTime;
    this.executionTime = executionTime;
    this.callback = callback;
  }

  public StageExecutorRequest getRequest() {
    return request;
  }

  public ZonedDateTime getStartTime() {
    return startTime;
  }

  public Duration getExecutionTime() {
    return executionTime;
  }

  public Function<StageExecutorRequest, StageExecutorResult> getCallback() {
    return callback;
  }
}
