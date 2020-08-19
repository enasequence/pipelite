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
import pipelite.entity.PipeliteProcess;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class PipeliteInMemoryLockServiceTest {

  private static final String processName = "TEST";

  @Test
  public void testLauncherLocks() {
    PipeliteInMemoryLockService locker = new PipeliteInMemoryLockService();

    String launcherName1 = "TEST1";
    String launcherName2 = "TEST2";

    locker.unlockLauncher(launcherName1, processName);
    locker.unlockLauncher(launcherName2, processName);

    assertTrue(locker.lockLauncher(launcherName1, processName));
    assertTrue(locker.isLauncherLocked(launcherName1, processName));

    assertTrue(locker.lockLauncher(launcherName2, processName));
    assertTrue(locker.isLauncherLocked(launcherName1, processName));
    assertTrue(locker.isLauncherLocked(launcherName2, processName));

    assertTrue(locker.unlockLauncher(launcherName1, processName));
    assertFalse(locker.isLauncherLocked(launcherName1, processName));
    assertTrue(locker.isLauncherLocked(launcherName2, processName));

    assertTrue(locker.unlockLauncher(launcherName2, processName));
    assertFalse(locker.isLauncherLocked(launcherName1, processName));
    assertFalse(locker.isLauncherLocked(launcherName2, processName));
  }

  @Test
  public void testProcessLocks() {
    PipeliteInMemoryLockService locker = new PipeliteInMemoryLockService();

    String launcherName1 = "TEST1";
    String launcherName2 = "TEST2";

    assertTrue(locker.lockProcess(launcherName1, getProcessInstance("1")));
    assertTrue(locker.isProcessLocked(getProcessInstance("1")));

    assertTrue(locker.lockProcess(launcherName1, getProcessInstance("2")));
    assertTrue(locker.isProcessLocked(getProcessInstance("1")));
    assertTrue(locker.isProcessLocked(getProcessInstance("2")));

    assertTrue(locker.unlockProcess(launcherName1, getProcessInstance("1")));
    assertFalse(locker.isProcessLocked(getProcessInstance("1")));
    assertTrue(locker.isProcessLocked(getProcessInstance("2")));

    assertTrue(locker.lockProcess(launcherName2, getProcessInstance("3")));
    assertFalse(locker.isProcessLocked(getProcessInstance("1")));
    assertTrue(locker.isProcessLocked(getProcessInstance("2")));
    assertTrue(locker.isProcessLocked(getProcessInstance("3")));

    assertTrue(locker.lockProcess(launcherName2, getProcessInstance("4")));
    assertFalse(locker.isProcessLocked(getProcessInstance("1")));
    assertTrue(locker.isProcessLocked(getProcessInstance("2")));
    assertTrue(locker.isProcessLocked(getProcessInstance("3")));
    assertTrue(locker.isProcessLocked(getProcessInstance("4")));

    assertTrue(locker.unlockProcess(launcherName2, getProcessInstance("4")));
    assertFalse(locker.isProcessLocked(getProcessInstance("1")));
    assertTrue(locker.isProcessLocked(getProcessInstance("2")));
    assertTrue(locker.isProcessLocked(getProcessInstance("3")));
    assertFalse(locker.isProcessLocked(getProcessInstance("4")));

    locker.purgeLauncherLocks(launcherName1, processName);

    assertFalse(locker.isProcessLocked(getProcessInstance("1")));
    assertFalse(locker.isProcessLocked(getProcessInstance("2")));
    assertTrue(locker.isProcessLocked(getProcessInstance("3")));
    assertFalse(locker.isProcessLocked(getProcessInstance("4")));

    locker.purgeLauncherLocks(launcherName2, processName);

    assertFalse(locker.isProcessLocked(getProcessInstance("1")));
    assertFalse(locker.isProcessLocked(getProcessInstance("2")));
    assertFalse(locker.isProcessLocked(getProcessInstance("3")));
    assertFalse(locker.isProcessLocked(getProcessInstance("4")));
  }

  private static PipeliteProcess getProcessInstance(String processId) {
    PipeliteProcess pipeliteProcess = new PipeliteProcess();
    pipeliteProcess.setProcessName(processName);
    pipeliteProcess.setProcessId(processId);
    return pipeliteProcess;
  }
}
