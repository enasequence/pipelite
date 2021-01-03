package pipelite.executor.task;

import org.junit.jupiter.api.Test;
import org.springframework.retry.support.RetryTemplate;
import pipelite.executor.task.RetryTask;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class RetryTaskTest {

  @Test
  public void fixedThrows() {
    int attempts = 3;
    AtomicInteger cnt = new AtomicInteger();
    RetryTemplate retry = RetryTask.fixed(Duration.ofMillis(1), attempts);
    assertThrows(
        RuntimeException.class,
        () -> {
          retry.execute(
              r -> {
                cnt.incrementAndGet();
                throw new RuntimeException();
              });
        });
    assertThat(cnt.get()).isEqualTo(attempts);
  }

  @Test
  public void fixedSuccess() {
    int attempts = 3;
    AtomicInteger cnt = new AtomicInteger();
    RetryTemplate retry = RetryTask.fixed(Duration.ofMillis(1), attempts);
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
        RetryTask.exponential(Duration.ofMillis(1), Duration.ofMillis(10), 2.0, attempts);
    assertThrows(
        RuntimeException.class,
        () -> {
          retry.execute(
              r -> {
                cnt.incrementAndGet();
                throw new RuntimeException();
              });
        });
    assertThat(cnt.get()).isEqualTo(attempts);
  }

  @Test
  public void exponentialSuccess() {
    int attempts = 3;
    AtomicInteger cnt = new AtomicInteger();
    RetryTemplate retry =
        RetryTask.exponential(Duration.ofMillis(1), Duration.ofMillis(10), 2.0, attempts);
    retry.execute(
        r -> {
          cnt.incrementAndGet();
          return null;
        });
    assertThat(cnt.get()).isEqualTo(1);
  }
}
