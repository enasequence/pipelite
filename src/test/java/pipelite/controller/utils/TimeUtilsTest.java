package pipelite.controller.utils;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.ZonedDateTime;

import static org.assertj.core.api.Assertions.assertThat;

public class TimeUtilsTest {

  @Test
  public void getDurationAsString() {
    ZonedDateTime now = ZonedDateTime.now();
    assertThat(
            TimeUtils.getDurationAsString(now, now.minus(Duration.between(now, now.plusHours(1)))))
        .isEqualTo("1h");
    assertThat(
            TimeUtils.getDurationAsString(
                now, now.minus(Duration.between(now, now.plusMinutes(30)))))
        .isEqualTo("30m");
    assertThat(
            TimeUtils.getDurationAsString(now, now.minus(Duration.between(now, now.minusHours(1)))))
        .isEqualTo("-1h");
    assertThat(
            TimeUtils.getDurationAsString(
                now, now.minus(Duration.between(now, now.minusMinutes(30)))))
        .isEqualTo("-30m");
  }

  @Test
  public void getDurationAsStringAlwaysPositive() {
    ZonedDateTime now = ZonedDateTime.now();
    assertThat(
            TimeUtils.getDurationAsStringAlwaysPositive(
                now, now.minus(Duration.between(now, now.plusHours(1)))))
        .isEqualTo("1h");
    assertThat(
            TimeUtils.getDurationAsStringAlwaysPositive(
                now, now.minus(Duration.between(now, now.plusMinutes(30)))))
        .isEqualTo("30m");
    assertThat(
            TimeUtils.getDurationAsStringAlwaysPositive(
                now, now.minus(Duration.between(now, now.minusHours(1)))))
        .isEqualTo("1h");
    assertThat(
            TimeUtils.getDurationAsStringAlwaysPositive(
                now, now.minus(Duration.between(now, now.minusMinutes(30)))))
        .isEqualTo("30m");
  }
}
