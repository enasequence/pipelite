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
package pipelite.retryable;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.springframework.retry.support.RetryTemplate;

public class RetryableExternalActionTest {

  @Test
  public void fixedThrows() {
    int attempts = 3;
    AtomicInteger cnt = new AtomicInteger();
    RetryTemplate retry =
        RetryableExternalAction.retryTemplate(
            RetryableExternalAction.fixedBackoffPolicy(Duration.ofMillis(1)),
            RetryableExternalAction.maxAttemptsRetryPolicy(attempts));

    assertThrows(
        RuntimeException.class,
        () ->
            retry.execute(
                r -> {
                  cnt.incrementAndGet();
                  throw new RuntimeException();
                }));
    assertThat(cnt.get()).isEqualTo(attempts);
  }

  @Test
  public void fixedSuccess() {
    int attempts = 3;
    AtomicInteger cnt = new AtomicInteger();
    RetryTemplate retry =
        RetryableExternalAction.retryTemplate(
            RetryableExternalAction.fixedBackoffPolicy(Duration.ofMillis(1)),
            RetryableExternalAction.maxAttemptsRetryPolicy(attempts));

    retry.execute(
        r -> {
          cnt.incrementAndGet();
          return null;
        });
    assertThat(cnt.get()).isEqualTo(1);
  }

  @Test
  public void exponentialThrows() {
    int attempts = 3;
    AtomicInteger cnt = new AtomicInteger();
    RetryTemplate retry =
        RetryableExternalAction.retryTemplate(
            RetryableExternalAction.expBackoffPolicy(
                Duration.ofMillis(1), Duration.ofMillis(10), 2.0),
            RetryableExternalAction.maxAttemptsRetryPolicy(attempts));

    assertThrows(
        RuntimeException.class,
        () ->
            retry.execute(
                r -> {
                  cnt.incrementAndGet();
                  throw new RuntimeException();
                }));
    assertThat(cnt.get()).isEqualTo(attempts);
  }

  @Test
  public void exponentialSuccess() {
    int attempts = 3;
    AtomicInteger cnt = new AtomicInteger();
    RetryTemplate retry =
        RetryableExternalAction.retryTemplate(
            RetryableExternalAction.expBackoffPolicy(
                Duration.ofMillis(1), Duration.ofMillis(10), 2.0),
            RetryableExternalAction.maxAttemptsRetryPolicy(attempts));

    retry.execute(
        r -> {
          cnt.incrementAndGet();
          return null;
        });
    assertThat(cnt.get()).isEqualTo(1);
  }
}
