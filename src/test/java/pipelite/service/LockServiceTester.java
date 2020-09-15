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
package pipelite.service;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import pipelite.UniqueStringGenerator;

public class LockServiceTester {

  private static final String pipelineName = UniqueStringGenerator.randomPipelineName();
  private static final String launcherName1 = UniqueStringGenerator.randomLauncherName();
  private static final String launcherName2 = UniqueStringGenerator.randomLauncherName();

  public static void testLaucherLocks(LockService service) {
    service.unlockLauncher(launcherName1, pipelineName);
    service.unlockLauncher(launcherName2, pipelineName);

    assertTrue(service.lockLauncher(launcherName1, pipelineName));
    assertTrue(service.isLauncherLocked(launcherName1, pipelineName));

    assertTrue(service.lockLauncher(launcherName2, pipelineName));
    assertTrue(service.isLauncherLocked(launcherName1, pipelineName));
    assertTrue(service.isLauncherLocked(launcherName2, pipelineName));

    assertTrue(service.unlockLauncher(launcherName1, pipelineName));
    assertFalse(service.isLauncherLocked(launcherName1, pipelineName));
    assertTrue(service.isLauncherLocked(launcherName2, pipelineName));

    assertTrue(service.unlockLauncher(launcherName2, pipelineName));
    assertFalse(service.isLauncherLocked(launcherName1, pipelineName));
    assertFalse(service.isLauncherLocked(launcherName2, pipelineName));
  }

  public static void testProcessLocks(LockService locker) {
    assertTrue(locker.lockProcess(launcherName1, pipelineName, "1"));
    assertTrue(locker.isProcessLocked(pipelineName, "1"));
    assertTrue(locker.isProcessLocked(launcherName1, pipelineName, "1"));

    assertTrue(locker.lockProcess(launcherName1, pipelineName, "2"));
    assertTrue(locker.isProcessLocked(pipelineName, "1"));
    assertTrue(locker.isProcessLocked(pipelineName, "2"));
    assertTrue(locker.isProcessLocked(launcherName1, pipelineName, "1"));
    assertTrue(locker.isProcessLocked(launcherName1, pipelineName, "2"));

    assertTrue(locker.unlockProcess(launcherName1, pipelineName, "1"));
    assertFalse(locker.isProcessLocked(pipelineName, "1"));
    assertTrue(locker.isProcessLocked(pipelineName, "2"));
    assertFalse(locker.isProcessLocked(launcherName1, pipelineName, "1"));
    assertTrue(locker.isProcessLocked(launcherName1, pipelineName, "2"));

    assertTrue(locker.lockProcess(launcherName2, pipelineName, "3"));
    assertFalse(locker.isProcessLocked(pipelineName, "1"));
    assertTrue(locker.isProcessLocked(pipelineName, "2"));
    assertTrue(locker.isProcessLocked(pipelineName, "3"));
    assertTrue(locker.isProcessLocked(launcherName1, pipelineName, "2"));
    assertFalse(locker.isProcessLocked(launcherName2, pipelineName, "2"));
    assertFalse(locker.isProcessLocked(launcherName1, pipelineName, "3"));
    assertTrue(locker.isProcessLocked(launcherName2, pipelineName, "3"));

    assertTrue(locker.lockProcess(launcherName2, pipelineName, "4"));
    assertFalse(locker.isProcessLocked(pipelineName, "1"));
    assertTrue(locker.isProcessLocked(pipelineName, "2"));
    assertTrue(locker.isProcessLocked(pipelineName, "3"));
    assertTrue(locker.isProcessLocked(pipelineName, "4"));

    assertTrue(locker.unlockProcess(launcherName2, pipelineName, "4"));
    assertFalse(locker.isProcessLocked(pipelineName, "1"));
    assertTrue(locker.isProcessLocked(pipelineName, "2"));
    assertTrue(locker.isProcessLocked(pipelineName, "3"));
    assertFalse(locker.isProcessLocked(pipelineName, "4"));

    locker.purgeLauncherLocks(launcherName1, pipelineName);

    assertFalse(locker.isProcessLocked(pipelineName, "1"));
    assertFalse(locker.isProcessLocked(pipelineName, "2"));
    assertTrue(locker.isProcessLocked(pipelineName, "3"));
    assertFalse(locker.isProcessLocked(pipelineName, "4"));

    locker.purgeLauncherLocks(launcherName2, pipelineName);

    assertFalse(locker.isProcessLocked(pipelineName, "1"));
    assertFalse(locker.isProcessLocked(pipelineName, "2"));
    assertFalse(locker.isProcessLocked(pipelineName, "3"));
    assertFalse(locker.isProcessLocked(pipelineName, "4"));
  }
}
