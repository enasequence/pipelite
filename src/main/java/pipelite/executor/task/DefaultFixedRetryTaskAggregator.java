package pipelite.executor.task;

import org.springframework.retry.support.RetryTemplate;

import java.time.Duration;

public class DefaultFixedRetryTaskAggregator<Request, Result, ExecutorContext>
    extends RetryTaskAggregator<Request, Result, ExecutorContext> {

  public static final Duration DEFAULT_REQUEST_TIMEOUT = Duration.ofMinutes(10);
  public static final RetryTemplate DEFAULT_REQUEST_RETRY = RetryTask.DEFAULT_FIXED;

  public DefaultFixedRetryTaskAggregator(
      int requestLimit,
      ExecutorContext executorContext,
      RetryTaskAggregatorCallback<Request, Result, ExecutorContext> task) {
    super(DEFAULT_REQUEST_RETRY, DEFAULT_REQUEST_TIMEOUT, requestLimit, executorContext, task);
  }
}
