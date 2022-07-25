/*
 * Copyright 2020-2022 EMBL - European Bioinformatics Institute
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package pipelite.retryable;

import java.time.Duration;
import lombok.extern.flogger.Flogger;
import org.springframework.retry.RetryPolicy;
import org.springframework.retry.backoff.BackOffPolicy;
import org.springframework.retry.backoff.ExponentialBackOffPolicy;
import org.springframework.retry.backoff.FixedBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;

@Flogger
public class RetryableExternalAction {

  private static final RetryTemplate RETRY_TEMPLATE =
      retryTemplate(fixedBackoffPolicy(Duration.ofSeconds(5)), maxAttemptsRetryPolicy(3));

  public static final <T, E extends Throwable> T execute(RetryableAction<T, E> action) throws E {
    return RETRY_TEMPLATE.execute(r -> action.get());
  }

  static RetryTemplate retryTemplate(BackOffPolicy backOffPolicy, RetryPolicy retryPolicy) {
    RetryTemplate retryTemplate = new RetryTemplate();
    retryTemplate.setBackOffPolicy(backOffPolicy);
    retryTemplate.setRetryPolicy(retryPolicy);
    return retryTemplate;
  }

  static FixedBackOffPolicy fixedBackoffPolicy(Duration backoff) {
    FixedBackOffPolicy backOffPolicy = new FixedBackOffPolicy();
    backOffPolicy.setBackOffPeriod(backoff.toMillis());
    return backOffPolicy;
  }

  static ExponentialBackOffPolicy expBackoffPolicy(
      Duration minBackoff, Duration maxBackoff, Double multiplier) {
    ExponentialBackOffPolicy backOffPolicy = new ExponentialBackOffPolicy();
    backOffPolicy.setInitialInterval(minBackoff.toMillis());
    backOffPolicy.setMaxInterval(maxBackoff.toMillis());
    backOffPolicy.setMultiplier(multiplier);
    return backOffPolicy;
  }

  static RetryPolicy maxAttemptsRetryPolicy(int attempts) {
    SimpleRetryPolicy retryPolicy = new SimpleRetryPolicy();
    retryPolicy.setMaxAttempts(attempts);
    return retryPolicy;
  }
}
