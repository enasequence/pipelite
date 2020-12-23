package pipelite.launcher.process.runner;

import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/** Simple time window counter. */
public class ProcessRunnerStatsCounter {
  private final Set<Event> events = ConcurrentHashMap.newKeySet();

  public static class Event {
    private final double count;
    private final ZonedDateTime time = ZonedDateTime.now();

    public Event(double count) {
      this.count = count;
    }
  }

  public void increment(double count) {
    if (count < 1) {
      return;
    }
    events.add(new Event(count));
  }

  /**
   * Returns the count within the given time window.
   *
   * @param since the start of the time window
   * @return the count within the given time window
   */
  public double getCount(Duration since) {
    double count = 0;
    for (Event event : events) {
      if (event.time.isAfter(ZonedDateTime.now().minus(since))) {
        count += event.count;
      }
    }
    return count;
  }

  /**
   * Removes counts outside the given time window.
   *
   * @param since the start of the time window
   */
  public void purge(Duration since) {
    for (Event event : events) {
      if (!event.time.isAfter(ZonedDateTime.now().minus(since))) {
        events.remove(event);
      }
    }
  }
}
