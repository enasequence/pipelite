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

import java.time.Duration;

import org.springframework.retry.backoff.ExponentialBackOffPolicy;
import org.springframework.retry.backoff.FixedBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;

/** Executes a task with retries using {@link RetryTemplate}. */
public class RetryTask {

  private final RetryTemplate retryTemplate;

  public RetryTask(RetryTemplate retryTemplate) {
    this.retryTemplate = retryTemplate;
  }

  public static final RetryTemplate DEFAULT_FIXED = RetryTask.fixed(Duration.ofSeconds(5), 3);
  public static final RetryTemplate DEFAULT_EXP =
      RetryTask.exp(Duration.ofSeconds(1), Duration.ofSeconds(15), 3.8, 3);

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

  public static final RetryTemplate exp(
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
