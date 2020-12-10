package pipelite.controller.utils;

import org.junit.jupiter.api.Test;
import pipelite.cron.CronUtils;

import java.time.Duration;
import java.time.ZonedDateTime;

import static org.assertj.core.api.Assertions.assertThat;

public class TimeUtilsTest {

  @Test
  public void getTimeAsString() {
    ZonedDateTime now = ZonedDateTime.now();
    assertThat(TimeUtils.getTimeAsString(now, now.minus(Duration.between(now, now.plusHours(1)))))
        .isEqualTo("1h");
    assertThat(
            TimeUtils.getTimeAsString(now, now.minus(Duration.between(now, now.plusMinutes(30)))))
        .isEqualTo("30m");
    assertThat(TimeUtils.getTimeAsString(now, now.minus(Duration.between(now, now.minusHours(1)))))
        .isEqualTo("-1h");
    assertThat(
            TimeUtils.getTimeAsString(now, now.minus(Duration.between(now, now.minusMinutes(30)))))
        .isEqualTo("-30m");
  }
}
