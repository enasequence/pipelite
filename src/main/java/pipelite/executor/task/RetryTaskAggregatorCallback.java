package pipelite.executor.task;

import java.util.List;
import java.util.Map;

/**
 * Callback interface for a task executed using {@link RetryTaskAggregator}.
 *
 * @param <ExecutorContext> the execution context
 * @param <Request>> the request
 * @param <Result>> the result returned by the task
 */
public interface RetryTaskAggregatorCallback<Request, Result, ExecutorContext> {

  /**
   * Execute a {@link RetryTaskAggregator} task.
   *
   * @param requests the requests
   * @param executorContext execution context
   * @return a map or requests and results
   */
  Map<Request, Result> execute(List<Request> requests, ExecutorContext executorContext);
}
