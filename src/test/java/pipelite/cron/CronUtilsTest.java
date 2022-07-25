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
package pipelite.cron;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import org.junit.jupiter.api.Test;

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
    assertThat(CronUtils.describe("invalid")).isEqualTo("invalid cron expression");
  }

  @Test
  public void testStandardCronLaunchTime() {
    // at minute
    assertThat(CronUtils.launchTime("1 * * * *", null).getMinute()).isEqualTo(1);
    assertThat(CronUtils.launchTime("1 * * * *", null).isAfter(ZonedDateTime.now()));
    assertThat(CronUtils.launchTime("1 * * * *", null).isBefore(ZonedDateTime.now().plusHours(1)));

    // at hour
    assertThat(CronUtils.launchTime("0 1 * * *", null).getHour()).isEqualTo(1);
    assertThat(CronUtils.launchTime("0 1 * * *", null).isAfter(ZonedDateTime.now()));
    assertThat(CronUtils.launchTime("0 1 * * *", null).isBefore(ZonedDateTime.now().plusDays(1)));

    // every hour
    ZonedDateTime now = ZonedDateTime.now();
    ZonedDateTime nowTruncatedToSeconds = now.truncatedTo(ChronoUnit.SECONDS);
    assertThat(CronUtils.launchTime("0 */1 * * *", null))
        .isBetween(nowTruncatedToSeconds, nowTruncatedToSeconds.plusHours(1));
    assertThat(CronUtils.launchTime("0 */1 * * *", nowTruncatedToSeconds.plusHours(1)))
        .isBetween(nowTruncatedToSeconds.plusHours(1), nowTruncatedToSeconds.plusHours(2));
    assertThat(CronUtils.launchTime("0 */1 * * *", nowTruncatedToSeconds.plusHours(2)))
        .isBetween(nowTruncatedToSeconds.plusHours(2), nowTruncatedToSeconds.plusHours(3));
  }
}
