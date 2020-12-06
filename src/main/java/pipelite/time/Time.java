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
