/*
 * Copyright 2020 EMBL - European Bioinformatics Institute
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package pipelite.executor.task;

import org.springframework.retry.RetryPolicy;
import org.springframework.retry.backoff.BackOffPolicy;
import org.springframework.retry.backoff.ExponentialBackOffPolicy;
import org.springframework.retry.backoff.FixedBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;

import java.time.Duration;

/** Execute a task with retries using {@link RetryTemplate}. */
public class RetryTask {

  private RetryTask() {}

  /**
   * The default retry policy makes three attempts separated by 5 seconds intervals. All exceptions
   * will be retried.
   */
  public static final RetryTemplate DEFAULT =
      RetryTask.retryTemplate(fixedBackoffPolicy(Duration.ofSeconds(5)), maxAttemptsRetryPolicy(3));

  public static RetryTemplate retryTemplate(BackOffPolicy backOffPolicy, RetryPolicy retryPolicy) {
    RetryTemplate retryTemplate = new RetryTemplate();
    retryTemplate.setBackOffPolicy(backOffPolicy);
    retryTemplate.setRetryPolicy(retryPolicy);
    return retryTemplate;
  }

  public static FixedBackOffPolicy fixedBackoffPolicy(Duration backoff) {
    FixedBackOffPolicy backOffPolicy = new FixedBackOffPolicy();
    backOffPolicy.setBackOffPeriod(backoff.toMillis());
    return backOffPolicy;
  }

  public static ExponentialBackOffPolicy expBackoffPolicy(
      Duration minBackoff, Duration maxBackoff, Double multiplier) {
    ExponentialBackOffPolicy backOffPolicy = new ExponentialBackOffPolicy();
    backOffPolicy.setInitialInterval(minBackoff.toMillis());
    backOffPolicy.setMaxInterval(maxBackoff.toMillis());
    backOffPolicy.setMultiplier(multiplier);
    return backOffPolicy;
  }

  public static RetryPolicy maxAttemptsRetryPolicy(int attempts) {
    SimpleRetryPolicy retryPolicy = new SimpleRetryPolicy();
    retryPolicy.setMaxAttempts(attempts);
    return retryPolicy;
  }
}
