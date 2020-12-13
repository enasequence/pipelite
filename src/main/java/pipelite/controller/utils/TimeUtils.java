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

public class TimeUtils {

  private TimeUtils() {}

  public static String getDurationAsString(ZonedDateTime startTime, ZonedDateTime endTime) {
    Duration duration = Duration.between(endTime, startTime);
    return duration.toString().substring(2).replaceAll("(\\d[HMS])(?!$)", "$1 ").toLowerCase();
  }

  public static String getDurationAsStringAlwaysPositive(
      ZonedDateTime startTime, ZonedDateTime endTime) {
    Duration duration = Duration.between(endTime, startTime);
    if (duration.isNegative()) {
      duration = Duration.between(startTime, endTime);
    }
    return duration.toString().substring(2).replaceAll("(\\d[HMS])(?!$)", "$1 ").toLowerCase();
  }
}
