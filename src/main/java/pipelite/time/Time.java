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
package pipelite.time;

import java.time.Duration;
import java.time.ZonedDateTime;
import pipelite.exception.PipeliteInterruptedException;
import pipelite.exception.PipeliteTimeoutException;

public class Time {

  private Time() {}

  /**
   * Waits the given duration.
   *
   * @param duration the wait duration
   * @throws pipelite.exception.PipeliteInterruptedException if the wait was interrupted
   */
  public static void wait(Duration duration) {
    try {
      Thread.sleep(duration.toMillis());
    } catch (InterruptedException ex) {
      Thread.interrupted();
      throw new PipeliteInterruptedException("Wait interrupted");
    }
  }

  /**
   * Waits the given duration but not past the given time.
   *
   * @param duration the wait duration
   * @param until wait only if the current time is not past the given time
   * @throws pipelite.exception.PipeliteInterruptedException if the wait was interrupted or if the
   *     current time was past the given time
   */
  public static void waitUntil(Duration duration, ZonedDateTime until) {
    if (until == null) {
      wait(duration);
    } else {
      if (ZonedDateTime.now().isAfter(until)) {
        throw new PipeliteTimeoutException("Wait timeout");
      }
      try {
        Thread.sleep(duration.toMillis());
      } catch (InterruptedException ex) {
        Thread.interrupted();
        throw new PipeliteInterruptedException("Wait interrupted");
      }
    }
  }
}
