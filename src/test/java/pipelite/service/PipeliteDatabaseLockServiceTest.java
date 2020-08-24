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
import pipelite.RandomStringGenerator;
import pipelite.FullTestConfiguration;

import org.springframework.transaction.annotation.Transactional;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest(classes = FullTestConfiguration.class)
@ActiveProfiles(value = {"database", "database-oracle-test"})
public class PipeliteDatabaseLockServiceTest {

  @Autowired PipeliteDatabaseLockService service;

  private static final String processName = RandomStringGenerator.randomProcessName();
  private static final String launcherName1 = RandomStringGenerator.randomLauncherName();
  private static final String launcherName2 = RandomStringGenerator.randomLauncherName();

  @Test
  @Transactional
  @Rollback
  public void test() {

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
  @Transactional
  @Rollback
  public void testProcessLocks() {

    service.purgeLauncherLocks(launcherName1, processName);
    service.purgeLauncherLocks(launcherName2, processName);

    assertTrue(service.lockProcess(launcherName1, processName, "PROCESSID1"));
    assertTrue(service.isProcessLocked(processName, "PROCESSID1"));

    assertTrue(service.lockProcess(launcherName1, processName, "PROCESSID2"));
    assertTrue(service.isProcessLocked(processName, "PROCESSID1"));
    assertTrue(service.isProcessLocked(processName, "PROCESSID2"));

    assertTrue(service.unlockProcess(launcherName1, processName, "PROCESSID1"));
    assertFalse(service.isProcessLocked(processName, "PROCESSID1"));
    assertTrue(service.isProcessLocked(processName, "PROCESSID2"));

    assertTrue(service.lockProcess(launcherName2, processName, "PROCESSID3"));
    assertFalse(service.isProcessLocked(processName, "PROCESSID1"));
    assertTrue(service.isProcessLocked(processName, "PROCESSID2"));
    assertTrue(service.isProcessLocked(processName, "PROCESSID3"));

    assertTrue(service.lockProcess(launcherName2, processName, "PROCESSID4"));
    assertFalse(service.isProcessLocked(processName, "PROCESSID1"));
    assertTrue(service.isProcessLocked(processName, "PROCESSID2"));
    assertTrue(service.isProcessLocked(processName, "PROCESSID3"));
    assertTrue(service.isProcessLocked(processName, "PROCESSID4"));

    assertTrue(service.unlockProcess(launcherName2, processName, "PROCESSID4"));
    assertFalse(service.isProcessLocked(processName, "PROCESSID1"));
    assertTrue(service.isProcessLocked(processName, "PROCESSID2"));
    assertTrue(service.isProcessLocked(processName, "PROCESSID3"));
    assertFalse(service.isProcessLocked(processName, "PROCESSID4"));

    service.purgeLauncherLocks(launcherName1, processName);

    assertFalse(service.isProcessLocked(processName, "PROCESSID1"));
    assertFalse(service.isProcessLocked(processName, "PROCESSID2"));
    assertTrue(service.isProcessLocked(processName, "PROCESSID3"));
    assertFalse(service.isProcessLocked(processName, "PROCESSID4"));

    service.purgeLauncherLocks(launcherName2, processName);

    assertFalse(service.isProcessLocked(processName, "PROCESSID1"));
    assertFalse(service.isProcessLocked(processName, "PROCESSID2"));
    assertFalse(service.isProcessLocked(processName, "PROCESSID3"));
    assertFalse(service.isProcessLocked(processName, "PROCESSID4"));
  }
}
