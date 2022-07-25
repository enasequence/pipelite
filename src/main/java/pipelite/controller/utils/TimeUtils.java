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
package pipelite.controller.utils;

import java.time.Duration;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

public class TimeUtils {

  private TimeUtils() {}

  private static final DateTimeFormatter dateTimeFormatter =
      DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ssX");

  /**
   * Returns the date time as a yyyy-MM-dd'T'HH:mm:ssX formatted string.
   *
   * @param dateTime the date time.
   * @return the date time as a yyyy-MM-dd'T'HH:mm:ssX formatted string
   */
  public static String humanReadableDate(ZonedDateTime dateTime) {
    if (dateTime == null) {
      return null;
    }
    return dateTime.format(dateTimeFormatter);
  }

  /**
   * Returns the duration between now and the date time as a [\d+d] [\d+h] [\d+m] [\d+s] formatted
   * string.
   *
   * @param dateTime the date time
   * @return the duration between now and the date time as a [\d+d] [\d+h] [\d+m] [\d+s] formatted
   *     string
   */
  public static String humanReadableDuration(ZonedDateTime dateTime) {
    if (dateTime == null) {
      return null;
    }
    return humanReadableDuration(dateTime, ZonedDateTime.now());
  }

  /**
   * Returns the duration between the two date times as a [\d+d] [\d+h] [\d+m] [\d+s] formatted
   * string.
   *
   * @param dateTime1 the first date time
   * @param dateTime2 the second date time
   * @return the duration between the two date times as a [\d+d] [\d+h] [\d+m] [\d+s] formatted
   *     string
   */
  public static String humanReadableDuration(ZonedDateTime dateTime1, ZonedDateTime dateTime2) {
    if (dateTime1 == null || dateTime2 == null) {
      return null;
    }
    return humanReadableDuration(Duration.between(dateTime2, dateTime1));
  }

  /**
   * Returns the duration between now and the date time as a [\d+d] [\d+h] [\d+m] [\d+s] formatted
   * string.
   *
   * @param duration the duration
   * @return the duration between now and the date time as a [\d+d] [\d+h] [\d+m] [\d+s] formatted
   *     string
   */
  public static String humanReadableDuration(Duration duration) {
    duration = duration.abs();
    long days = duration.toDays();
    duration = duration.minusDays(days);
    long hours = duration.toHours();
    duration = duration.minusHours(hours);
    long minutes = duration.toMinutes();
    duration = duration.minusMinutes(minutes);
    long seconds = duration.getSeconds();
    String str =
        ((days == 0 ? "" : days + "d ")
                + (hours == 0 ? "" : hours + "h ")
                + (minutes == 0 ? "" : minutes + "m ")
                + (seconds == 0 ? "" : seconds + "s "))
            .trim();
    if (str == null) {
      str = "0s";
    }
    return str;
  }
}
