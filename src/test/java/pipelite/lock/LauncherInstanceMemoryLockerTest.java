/*
 * Copyright 2018-2019 EMBL - European Bioinformatics Institute
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package pipelite.lock;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class LauncherInstanceMemoryLockerTest {

  static final private String processName = "TEST";

  @Test
  public void test() {
    LauncherInstanceMemoryLocker locker = new LauncherInstanceMemoryLocker();

    String launcherId1 = "TEST1";
    String launcherId2 = "TEST2";

    locker.unlock(launcherId1, processName);
    locker.unlock(launcherId2, processName);

    assertTrue(locker.lock(launcherId1, processName));
    assertTrue(locker.isLocked(launcherId1, processName));

    assertTrue(locker.lock(launcherId2, processName));
    assertTrue(locker.isLocked(launcherId1, processName));
    assertTrue(locker.isLocked(launcherId2, processName));

    assertTrue(locker.unlock(launcherId1, processName));
    assertFalse(locker.isLocked(launcherId1, processName));
    assertTrue(locker.isLocked(launcherId2, processName));

    assertTrue(locker.unlock(launcherId2, processName));
    assertFalse(locker.isLocked(launcherId1, processName));
    assertFalse(locker.isLocked(launcherId2, processName));
  }
}
