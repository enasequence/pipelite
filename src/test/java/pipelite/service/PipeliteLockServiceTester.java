package pipelite.service;

import pipelite.UniqueStringGenerator;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class PipeliteLockServiceTester {

  private static final String processName = UniqueStringGenerator.randomProcessName();
  private static final String launcherName1 = UniqueStringGenerator.randomLauncherName();
  private static final String launcherName2 = UniqueStringGenerator.randomLauncherName();

  public static void testLaucherLocks(PipeliteLockService service) {
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

  public static void testProcessLocks(PipeliteLockService locker) {
    assertTrue(locker.lockProcess(launcherName1, processName, "1"));
    assertTrue(locker.isProcessLocked(processName, "1"));
    assertTrue(locker.isProcessLocked(launcherName1, processName, "1"));

    assertTrue(locker.lockProcess(launcherName1, processName, "2"));
    assertTrue(locker.isProcessLocked(processName, "1"));
    assertTrue(locker.isProcessLocked(processName, "2"));
    assertTrue(locker.isProcessLocked(launcherName1, processName, "1"));
    assertTrue(locker.isProcessLocked(launcherName1, processName, "2"));

    assertTrue(locker.unlockProcess(launcherName1, processName, "1"));
    assertFalse(locker.isProcessLocked(processName, "1"));
    assertTrue(locker.isProcessLocked(processName, "2"));
    assertFalse(locker.isProcessLocked(launcherName1, processName, "1"));
    assertTrue(locker.isProcessLocked(launcherName1, processName, "2"));

    assertTrue(locker.lockProcess(launcherName2, processName, "3"));
    assertFalse(locker.isProcessLocked(processName, "1"));
    assertTrue(locker.isProcessLocked(processName, "2"));
    assertTrue(locker.isProcessLocked(processName, "3"));
    assertTrue(locker.isProcessLocked(launcherName1, processName, "2"));
    assertFalse(locker.isProcessLocked(launcherName2, processName, "2"));
    assertFalse(locker.isProcessLocked(launcherName1, processName, "3"));
    assertTrue(locker.isProcessLocked(launcherName2, processName, "3"));

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
