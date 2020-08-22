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
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.Rollback;
import org.springframework.test.context.ActiveProfiles;
import pipelite.TestConfiguration;
import pipelite.entity.PipeliteProcess;

import javax.transaction.Transactional;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest(classes = TestConfiguration.class)
@ActiveProfiles("test")
public class PipeliteDatabaseLockServiceTest {

  private static final String processName = "PROCESS1";

  @Autowired PipeliteDatabaseLockService locker;

  @Test
  @Transactional
  @Rollback
  public void test() {

    String launcherName1 = "LAUNCHER1";
    String launcherName2 = "LAUNCHER2";

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
  @Transactional
  @Rollback
  public void testProcessLocks() {

    String launcherName1 = "LAUNCHER1";
    String launcherName2 = "LAUNCHER2";

    locker.purgeLauncherLocks(launcherName1, processName);
    locker.purgeLauncherLocks(launcherName2, processName);

    assertTrue(locker.lockProcess(launcherName1, getProcessInstance("PROCESSID1")));
    assertTrue(locker.isProcessLocked(getProcessInstance("PROCESSID1")));

    assertTrue(locker.lockProcess(launcherName1, getProcessInstance("PROCESSID2")));
    assertTrue(locker.isProcessLocked(getProcessInstance("PROCESSID1")));
    assertTrue(locker.isProcessLocked(getProcessInstance("PROCESSID2")));

    assertTrue(locker.unlockProcess(launcherName1, getProcessInstance("PROCESSID1")));
    assertFalse(locker.isProcessLocked(getProcessInstance("PROCESSID1")));
    assertTrue(locker.isProcessLocked(getProcessInstance("PROCESSID2")));

    assertTrue(locker.lockProcess(launcherName2, getProcessInstance("PROCESSID3")));
    assertFalse(locker.isProcessLocked(getProcessInstance("PROCESSID1")));
    assertTrue(locker.isProcessLocked(getProcessInstance("PROCESSID2")));
    assertTrue(locker.isProcessLocked(getProcessInstance("PROCESSID3")));

    assertTrue(locker.lockProcess(launcherName2, getProcessInstance("PROCESSID4")));
    assertFalse(locker.isProcessLocked(getProcessInstance("PROCESSID1")));
    assertTrue(locker.isProcessLocked(getProcessInstance("PROCESSID2")));
    assertTrue(locker.isProcessLocked(getProcessInstance("PROCESSID3")));
    assertTrue(locker.isProcessLocked(getProcessInstance("PROCESSID4")));

    assertTrue(locker.unlockProcess(launcherName2, getProcessInstance("PROCESSID4")));
    assertFalse(locker.isProcessLocked(getProcessInstance("PROCESSID1")));
    assertTrue(locker.isProcessLocked(getProcessInstance("PROCESSID2")));
    assertTrue(locker.isProcessLocked(getProcessInstance("PROCESSID3")));
    assertFalse(locker.isProcessLocked(getProcessInstance("PROCESSID4")));

    locker.purgeLauncherLocks(launcherName1, processName);

    assertFalse(locker.isProcessLocked(getProcessInstance("PROCESSID1")));
    assertFalse(locker.isProcessLocked(getProcessInstance("PROCESSID2")));
    assertTrue(locker.isProcessLocked(getProcessInstance("PROCESSID3")));
    assertFalse(locker.isProcessLocked(getProcessInstance("PROCESSID4")));

    locker.purgeLauncherLocks(launcherName2, processName);

    assertFalse(locker.isProcessLocked(getProcessInstance("PROCESSID1")));
    assertFalse(locker.isProcessLocked(getProcessInstance("PROCESSID2")));
    assertFalse(locker.isProcessLocked(getProcessInstance("PROCESSID3")));
    assertFalse(locker.isProcessLocked(getProcessInstance("PROCESSID4")));
  }

  private static PipeliteProcess getProcessInstance(String processId) {
    PipeliteProcess pipeliteProcess = new PipeliteProcess();
    pipeliteProcess.setProcessName(processName);
    pipeliteProcess.setProcessId(processId);
    return pipeliteProcess;
  }
}
