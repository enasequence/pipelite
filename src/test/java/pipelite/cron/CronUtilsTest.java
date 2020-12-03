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
package pipelite.cron;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.LocalDateTime;
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
  public void testLaunchTime() {
    assertThat(CronUtils.launchTime("00 11 * * *").getHour()).isEqualTo(11);
    assertThat(CronUtils.launchTime("00 11 * * *").isAfter(LocalDateTime.now()));
    assertThat(CronUtils.launchTime("00 11 * * *").isBefore(LocalDateTime.now().plusDays(1)));
  }
}
