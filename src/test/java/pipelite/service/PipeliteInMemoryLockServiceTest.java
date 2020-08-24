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
package pipelite.service;

import org.junit.jupiter.api.Test;
import pipelite.UniqueStringGenerator;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class PipeliteInMemoryLockServiceTest {

  private static final String processName = UniqueStringGenerator.randomProcessName();
  private static final String launcherName1 = UniqueStringGenerator.randomLauncherName();
  private static final String launcherName2 = UniqueStringGenerator.randomLauncherName();

  @Test
  public void testLauncherLocks() {
    PipeliteInMemoryLockService service = new PipeliteInMemoryLockService();

    service.unlockLauncher(launcherName1, processName);
    service.unlockLauncher(launcherName2, processName);

    assertTrue(service.lockLauncher(launcherName1, processName));
    assertTrue(service.isLauncherLocked(launcherName1, processName));

    assertTrue(service.lockLauncher(launcherName2, processName));
    assertTrue(service.isLauncherLocked(launcherName1, processName));
    assertTrue(service.isLauncherLocked(launcherName2, processName));

    assertTrue(service.unlockLauncher(launcherName1, processName));
    assertFalse(service.isLauncherLocked(launcherName1, processName));
    assertTrue(service.isLauncherLocked(launcherName2, processName));

    assertTrue(service.unlockLauncher(launcherName2, processName));
    assertFalse(service.isLauncherLocked(launcherName1, processName));
    assertFalse(service.isLauncherLocked(launcherName2, processName));
  }

  @Test
  public void testProcessLocks() {
    PipeliteInMemoryLockService locker = new PipeliteInMemoryLockService();

    assertTrue(locker.lockProcess(launcherName1, processName, "1"));
    assertTrue(locker.isProcessLocked(processName, "1"));

    assertTrue(locker.lockProcess(launcherName1, processName, "2"));
    assertTrue(locker.isProcessLocked(processName, "1"));
    assertTrue(locker.isProcessLocked(processName, "2"));

    assertTrue(locker.unlockProcess(launcherName1, processName, "1"));
    assertFalse(locker.isProcessLocked(processName, "1"));
    assertTrue(locker.isProcessLocked(processName, "2"));

    assertTrue(locker.lockProcess(launcherName2, processName, "3"));
    assertFalse(locker.isProcessLocked(processName, "1"));
    assertTrue(locker.isProcessLocked(processName, "2"));
    assertTrue(locker.isProcessLocked(processName, "3"));

    assertTrue(locker.lockProcess(launcherName2, processName, "4"));
    assertFalse(locker.isProcessLocked(processName, "1"));
    assertTrue(locker.isProcessLocked(processName, "2"));
    assertTrue(locker.isProcessLocked(processName, "3"));
    assertTrue(locker.isProcessLocked(processName, "4"));

    assertTrue(locker.unlockProcess(launcherName2, processName, "4"));
    assertFalse(locker.isProcessLocked(processName, "1"));
    assertTrue(locker.isProcessLocked(processName, "2"));
    assertTrue(locker.isProcessLocked(processName, "3"));
    assertFalse(locker.isProcessLocked(processName, "4"));

    locker.purgeLauncherLocks(launcherName1, processName);

    assertFalse(locker.isProcessLocked(processName, "1"));
    assertFalse(locker.isProcessLocked(processName, "2"));
    assertTrue(locker.isProcessLocked(processName, "3"));
    assertFalse(locker.isProcessLocked(processName, "4"));

    locker.purgeLauncherLocks(launcherName2, processName);

    assertFalse(locker.isProcessLocked(processName, "1"));
    assertFalse(locker.isProcessLocked(processName, "2"));
    assertFalse(locker.isProcessLocked(processName, "3"));
    assertFalse(locker.isProcessLocked(processName, "4"));
  }
}
