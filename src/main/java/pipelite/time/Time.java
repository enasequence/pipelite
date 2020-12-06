package pipelite.time;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

public class Time {

  private Time() {}

  /**
   * Waits the given duration.
   *
   * @param duration the duration
   * @param timeUnit the time unit
   * @return true if the wait was not interrupted
   */
  public static boolean wait(long duration, TimeUnit timeUnit) {
    try {
      timeUnit.sleep(duration);
      return true;
    } catch (InterruptedException ex) {
      Thread.interrupted();
      return false;
    }
  }

  /**
   * Waits the given duration.
   *
   * @param duration the duration
   * @return true if the wait was not interrupted
   */
  public static boolean wait(Duration duration) {
    try {
      Thread.sleep(duration.toMillis());
      return true;
    } catch (InterruptedException ex) {
      Thread.interrupted();
      return false;
    }
  }
}
