package pipelite.launcher;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class PipeliteSchedulerTest {

  @Test
  public void getNextProcessId() {
    assertThat(PipeliteScheduler.getNextProcessId(null)).isEqualTo("1");
    assertThat(PipeliteScheduler.getNextProcessId("0")).isEqualTo("1");
    assertThat(PipeliteScheduler.getNextProcessId("1")).isEqualTo("2");
    assertThat(PipeliteScheduler.getNextProcessId("9")).isEqualTo("10");
    assertThat(PipeliteScheduler.getNextProcessId("10")).isEqualTo("11");
    assertThat(PipeliteScheduler.getNextProcessId("29")).isEqualTo("30");
    assertThat(PipeliteScheduler.getNextProcessId("134232")).isEqualTo("134233");
  }
}
