package pipelite.cron;

import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;

import static org.assertj.core.api.Assertions.assertThat;

public class CronUtilsTest {

  @Test
  public void testValidate() {
    assertThat(CronUtils.validate("00 11,16 * * *")).isTrue();
    assertThat(CronUtils.validate("00 09-18 * * *")).isTrue();
    assertThat(CronUtils.validate("00 09-18 * * 1-5")).isTrue();
    assertThat(CronUtils.validate("*/10 * * * *")).isTrue();
    assertThat(CronUtils.validate("invalid")).isFalse();
  }

  @Test
  public void testDescribe() {
    assertThat(CronUtils.describe("00 11,16 * * *")).isEqualTo("at 11 and 16 hours");
    assertThat(CronUtils.describe("00 09-18 * * *")).isEqualTo("every hour between 9 and 18");
    assertThat(CronUtils.describe("00 09-18 * * 1-5"))
        .isEqualTo("every hour between 9 and 18 every day between Monday and Friday");
    assertThat(CronUtils.describe("*/10 * * * *")).isEqualTo("every 10 minutes");
  }

  @Test
  public void testLaunchTime() {
    assertThat(CronUtils.launchTime("00 11 * * *").getHour()).isEqualTo(11);
    assertThat(CronUtils.launchTime("00 11 * * *").isAfter(LocalDateTime.now()));
    assertThat(CronUtils.launchTime("00 11 * * *").isBefore(LocalDateTime.now().plusDays(1)));
  }
}
