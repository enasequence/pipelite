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
package pipelite.controller.utils;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.ZonedDateTime;
import org.junit.jupiter.api.Test;

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
