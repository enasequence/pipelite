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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.concurrent.TimeUnit;

import pipelite.UniqueStringGenerator;
import pipelite.entity.LauncherLockEntity;
import pipelite.launcher.process.runner.ProcessRunnerType;
import pipelite.time.Time;

public class LockServiceTester {

  public static void testLauncherLocks(LockService service) {
    String launcherName1 = UniqueStringGenerator.randomLauncherName();
    String launcherName2 = UniqueStringGenerator.randomLauncherName();

    service.getLauncherLocksByLauncherName(launcherName1).forEach(s -> service.unlockLauncher(s));
    service.getLauncherLocksByLauncherName(launcherName2).forEach(s -> service.unlockLauncher(s));

    LauncherLockEntity launcherLock1 =
        service.lockLauncher(launcherName1, ProcessRunnerType.LAUNCHER);
    LauncherLockEntity launcherLock2 =
        service.lockLauncher(launcherName2, ProcessRunnerType.LAUNCHER);

    ZonedDateTime expiry1 = launcherLock1.getExpiry();
    ZonedDateTime expiry2 = launcherLock2.getExpiry();

    assertThat(launcherLock1.getLauncherId()).isGreaterThan(0);
    assertThat(launcherLock1.getLauncherName()).isEqualTo(launcherName1);
    assertThat(launcherLock1.getExpiry())
        .isAfterOrEqualTo(
            ZonedDateTime.now().plus(service.getLockDuration()).minus(Duration.ofSeconds(10)));

    assertThat(launcherLock2.getLauncherId()).isGreaterThan(0);
    assertThat(launcherLock2.getLauncherName()).isEqualTo(launcherName2);
    assertThat(launcherLock2.getExpiry())
        .isAfterOrEqualTo(
            ZonedDateTime.now().plus(service.getLockDuration()).minus(Duration.ofSeconds(10)));

    assertThat(launcherLock1.getLauncherId()).isLessThan(launcherLock2.getLauncherId());

    assertThat(service.getLauncherLocksByLauncherName(launcherName1).size()).isOne();
    assertThat(service.getLauncherLocksByLauncherName(launcherName2).size()).isOne();
    assertThat(service.getLauncherLocksByLauncherName(launcherName1).get(0).getLauncherId())
        .isEqualTo(launcherLock1.getLauncherId());
    assertThat(service.getLauncherLocksByLauncherName(launcherName2).get(0).getLauncherId())
        .isEqualTo(launcherLock2.getLauncherId());

    assertTrue(service.relockLauncher(launcherLock1));
    assertTrue(service.relockLauncher(launcherLock2));

    assertThat(service.getLauncherLocksByLauncherName(launcherName1).size()).isOne();
    assertThat(service.getLauncherLocksByLauncherName(launcherName2).size()).isOne();
    assertThat(service.getLauncherLocksByLauncherName(launcherName1).get(0).getLauncherId())
        .isEqualTo(launcherLock1.getLauncherId());
    assertThat(service.getLauncherLocksByLauncherName(launcherName2).get(0).getLauncherId())
        .isEqualTo(launcherLock2.getLauncherId());
    assertThat(service.getLauncherLocksByLauncherName(launcherName1).get(0).getExpiry())
        .isAfterOrEqualTo(expiry1);
    assertThat(service.getLauncherLocksByLauncherName(launcherName2).get(0).getExpiry())
        .isAfterOrEqualTo(expiry2);

    service.unlockLauncher(launcherLock1);
    service.unlockLauncher(launcherLock2);
    assertThat(service.getLauncherLocksByLauncherName(launcherName1).size()).isZero();
    assertThat(service.getLauncherLocksByLauncherName(launcherName2).size()).isZero();
  }

  public static void testProcessLocks(LockService service) {
    String pipelineName = UniqueStringGenerator.randomPipelineName();
    String launcherName1 = UniqueStringGenerator.randomLauncherName();
    String launcherName2 = UniqueStringGenerator.randomLauncherName();

    service.getLauncherLocksByLauncherName(launcherName1).forEach(s -> service.unlockLauncher(s));
    service.getLauncherLocksByLauncherName(launcherName2).forEach(s -> service.unlockLauncher(s));

    LauncherLockEntity launcherLock1 =
        service.lockLauncher(launcherName1, ProcessRunnerType.LAUNCHER);
    LauncherLockEntity launcherLock2 =
        service.lockLauncher(launcherName2, ProcessRunnerType.LAUNCHER);

    assertTrue(service.lockProcess(launcherLock1, pipelineName, "1"));
    assertTrue(service.isProcessLocked(pipelineName, "1"));

    assertTrue(service.lockProcess(launcherLock1, pipelineName, "2"));
    assertTrue(service.isProcessLocked(pipelineName, "1"));
    assertTrue(service.isProcessLocked(pipelineName, "2"));

    assertTrue(service.unlockProcess(launcherLock1, pipelineName, "1"));
    assertFalse(service.isProcessLocked(pipelineName, "1"));
    assertTrue(service.isProcessLocked(pipelineName, "2"));

    assertTrue(service.lockProcess(launcherLock2, pipelineName, "3"));
    assertFalse(service.isProcessLocked(pipelineName, "1"));
    assertTrue(service.isProcessLocked(pipelineName, "2"));
    assertTrue(service.isProcessLocked(pipelineName, "3"));

    assertTrue(service.lockProcess(launcherLock2, pipelineName, "4"));
    assertFalse(service.isProcessLocked(pipelineName, "1"));
    assertTrue(service.isProcessLocked(pipelineName, "2"));
    assertTrue(service.isProcessLocked(pipelineName, "3"));
    assertTrue(service.isProcessLocked(pipelineName, "4"));

    assertTrue(service.unlockProcess(launcherLock2, pipelineName, "4"));
    assertFalse(service.isProcessLocked(pipelineName, "1"));
    assertTrue(service.isProcessLocked(pipelineName, "2"));
    assertTrue(service.isProcessLocked(pipelineName, "3"));
    assertFalse(service.isProcessLocked(pipelineName, "4"));

    service.unlockProcesses(launcherLock1);

    assertFalse(service.isProcessLocked(pipelineName, "1"));
    assertFalse(service.isProcessLocked(pipelineName, "2"));
    assertTrue(service.isProcessLocked(pipelineName, "3"));
    assertFalse(service.isProcessLocked(pipelineName, "4"));

    service.unlockProcesses(launcherLock2);

    assertFalse(service.isProcessLocked(pipelineName, "1"));
    assertFalse(service.isProcessLocked(pipelineName, "2"));
    assertFalse(service.isProcessLocked(pipelineName, "3"));
    assertFalse(service.isProcessLocked(pipelineName, "4"));
  }

  // Test fails if the lock is not created and checked for the first time within the lock duration.
  public static void testRemoveExpiredLauncherLock(LockService service) {
    String launcherName1 = UniqueStringGenerator.randomLauncherName();
    String launcherName2 = UniqueStringGenerator.randomLauncherName();

    service.getLauncherLocksByLauncherName(launcherName1).forEach(s -> service.unlockLauncher(s));
    service.getLauncherLocksByLauncherName(launcherName2).forEach(s -> service.unlockLauncher(s));

    assertThat(service.lockLauncher(launcherName1, ProcessRunnerType.LAUNCHER)).isNotNull();
    assertThat(service.isLauncherLocked(launcherName1)).isTrue();

    // Expired lock will not be removed.
    assertThat(service.lockLauncher(launcherName1, ProcessRunnerType.LAUNCHER)).isNull();
    assertThat(service.isLauncherLocked(launcherName1)).isTrue();

    Time.wait(service.getLockDuration());

    // Expired lock will be removed.
    assertThat(service.lockLauncher(launcherName1, ProcessRunnerType.LAUNCHER)).isNotNull();
    assertThat(service.isLauncherLocked(launcherName1)).isTrue();
  }

  // Test fails if the lock is not created and checked for the first time within the lock duration.
  public static void testRemoveExpiredProcessLock(LockService service) {
    String pipelineName = UniqueStringGenerator.randomPipelineName();
    String launcherName1 = UniqueStringGenerator.randomLauncherName();
    String launcherName2 = UniqueStringGenerator.randomLauncherName();

    service.getLauncherLocksByLauncherName(launcherName1).forEach(s -> service.unlockLauncher(s));
    service.getLauncherLocksByLauncherName(launcherName2).forEach(s -> service.unlockLauncher(s));

    LauncherLockEntity launcherLock1 =
        service.lockLauncher(launcherName1, ProcessRunnerType.LAUNCHER);
    LauncherLockEntity launcherLock2 =
        service.lockLauncher(launcherName2, ProcessRunnerType.LAUNCHER);

    assertThat(service.isLauncherLocked(launcherName1)).isTrue();
    assertThat(service.isLauncherLocked(launcherName2)).isTrue();

    String processId = "1";

    assertThat(service.isProcessLocked(pipelineName, processId)).isFalse();
    assertThat(service.lockProcess(launcherLock1, pipelineName, processId)).isTrue();
    assertThat(service.lockProcess(launcherLock2, pipelineName, processId)).isFalse();
    assertThat(service.isProcessLocked(pipelineName, processId)).isTrue();

    // Expired lock will not be removed.
    assertThat(service.lockProcess(launcherLock2, pipelineName, processId)).isFalse();
    assertThat(service.isProcessLocked(pipelineName, processId)).isTrue();

    Time.wait(service.getLockDuration());

    // Expired lock will be removed.
    service.relockLauncher(launcherLock2);
    assertThat(service.lockProcess(launcherLock2, pipelineName, processId)).isTrue();
    assertThat(service.isProcessLocked(pipelineName, processId)).isTrue();
  }
}
