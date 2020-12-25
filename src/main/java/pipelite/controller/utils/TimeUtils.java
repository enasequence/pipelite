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

import java.time.Duration;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;

public class TimeUtils {

  private TimeUtils() {}

  private static final DateTimeFormatter dateTimeFormatter =
      DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ssX");

  public static String dateTimeToString(ZonedDateTime dateTime) {
    return dateTime.format(dateTimeFormatter);
  }

  public static String durationToString(ZonedDateTime startTime, ZonedDateTime endTime) {
    Duration duration = Duration.between(endTime, startTime);
    return duration.toString().substring(2).replaceAll("(\\d[HMS])(?!$)", "$1 ").toLowerCase();
  }

  public static String durationToStringAlwaysPositive(
      ZonedDateTime startTime, ZonedDateTime endTime) {
    Duration duration = Duration.between(endTime, startTime);
    if (duration.isNegative()) {
      duration = Duration.between(startTime, endTime);
    }
    return duration.toString().substring(2).replaceAll("(\\d[HMS])(?!$)", "$1 ").toLowerCase();
  }
}
