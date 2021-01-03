package pipelite.executor.task;

import org.springframework.retry.backoff.ExponentialBackOffPolicy;
import org.springframework.retry.backoff.FixedBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;

import java.time.Duration;

/** Creates retry templates. */
public class RetryTask {

  private RetryTask() {}

  public static final RetryTemplate fixed(Duration backoff, int attempts) {
    RetryTemplate retryTemplate = new RetryTemplate();
    FixedBackOffPolicy backOffPolicy = new FixedBackOffPolicy();
    backOffPolicy.setBackOffPeriod(backoff.toMillis());
    retryTemplate.setBackOffPolicy(backOffPolicy);
    SimpleRetryPolicy retryPolicy = new SimpleRetryPolicy();
    retryPolicy.setMaxAttempts(attempts);
    retryTemplate.setRetryPolicy(retryPolicy);
    return retryTemplate;
  }

  public static final RetryTemplate exponential(
      Duration minBackoff, Duration maxBackoff, Double multiplier, int attempts) {
    RetryTemplate retryTemplate = new RetryTemplate();
    ExponentialBackOffPolicy backOffPolicy = new ExponentialBackOffPolicy();
    backOffPolicy.setInitialInterval(minBackoff.toMillis());
    backOffPolicy.setMaxInterval(maxBackoff.toMillis());
    backOffPolicy.setMultiplier(multiplier);
    retryTemplate.setBackOffPolicy(backOffPolicy);
    SimpleRetryPolicy retryPolicy = new SimpleRetryPolicy();
    retryPolicy.setMaxAttempts(attempts);
    retryTemplate.setRetryPolicy(retryPolicy);
    return retryTemplate;
  }
}
