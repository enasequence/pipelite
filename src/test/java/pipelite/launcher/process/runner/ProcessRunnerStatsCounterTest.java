package pipelite.launcher.process.runner;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

public class ProcessRunnerStatsCounterTest {

  @Test
  public void test() {
    ProcessRunnerStatsCounter counter = new ProcessRunnerStatsCounter();

    Duration since = Duration.ofDays(1);

    assertThat(counter.getCount(since)).isZero();

    counter.increment(1);
    assertThat(counter.getCount(since)).isEqualTo(1);

    counter.increment(2);
    assertThat(counter.getCount(since)).isEqualTo(3);

    counter.increment(3);
    assertThat(counter.getCount(since)).isEqualTo(6);

    counter.purge(since);

    assertThat(counter.getCount(since)).isEqualTo(6);

    counter.purge(Duration.ofDays(0));

    assertThat(counter.getCount(since)).isEqualTo(0);
  }
}
