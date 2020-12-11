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
