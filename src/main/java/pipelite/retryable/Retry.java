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
import java.util.concurrent.atomic.AtomicReference;
import lombok.extern.flogger.Flogger;
import org.springframework.retry.RetryPolicy;
import org.springframework.retry.backoff.BackOffPolicy;
import org.springframework.retry.backoff.FixedBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;
import pipelite.executor.cmd.CmdRunner;
import pipelite.stage.executor.StageExecutorResult;

@Flogger
public class Retry {

  private static class RetryException extends RuntimeException {
    public RetryException(String message) {
      super(message);
    }
  }

  /** Default retry. */
  public static final Retry DEFAULT = fixed();

  private final RetryTemplate retryTemplate;

  private Retry(RetryTemplate retryTemplate) {
    this.retryTemplate = retryTemplate;
  }

  /** Default fixed retry template. */
  public static Retry fixed() {
    return fixed(Duration.ofSeconds(5), 3);
  }

  /** Custom fixed retry template. */
  public static Retry fixed(Duration backoff, int attempts) {
    return new Retry(retryTemplate(fixedBackoffPolicy(backoff), maxAttemptsRetryPolicy(attempts)));
  }

  /** Execute retryable action. */
  public <T, E extends Throwable> T execute(final RetryAction<T, E> action) throws E {
    return retryTemplate.execute(r -> action.execute());
  }

  /** Execute retryable action. */
  public <E extends Throwable> StageExecutorResult execute(
      final CmdRunner cmdRunner, final String cmd) throws E {
    AtomicReference<StageExecutorResult> result = new AtomicReference<>();
    try {
      return retryTemplate.execute(
          r -> {
            result.set(cmdRunner.execute(cmd));
            if (result.get().isError()) {
              log.atSevere().log(
                  "Failed to execute retryable command: " + cmd + ": " + result.get().stageLog());
              throw new RetryException("Failed to execute retryable command: " + cmd);
            }
            return result.get();
          });
    } catch (RetryException ex) {
      log.atSevere().log("No more retries left to execute retryable command: " + cmd);
    }
    return result.get();
  }

  private static RetryTemplate retryTemplate(BackOffPolicy backOffPolicy, RetryPolicy retryPolicy) {
    RetryTemplate retryTemplate = new RetryTemplate();
    retryTemplate.setBackOffPolicy(backOffPolicy);
    retryTemplate.setRetryPolicy(retryPolicy);
    return retryTemplate;
  }

  private static FixedBackOffPolicy fixedBackoffPolicy(Duration backoff) {
    FixedBackOffPolicy backOffPolicy = new FixedBackOffPolicy();
    backOffPolicy.setBackOffPeriod(backoff.toMillis());
    return backOffPolicy;
  }

  private static RetryPolicy maxAttemptsRetryPolicy(int attempts) {
    SimpleRetryPolicy retryPolicy = new SimpleRetryPolicy();
    retryPolicy.setMaxAttempts(attempts);
    return retryPolicy;
  }
}
