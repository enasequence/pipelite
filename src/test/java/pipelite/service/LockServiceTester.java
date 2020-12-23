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
import pipelite.UniqueStringGenerator;
import pipelite.entity.LauncherLockEntity;
import pipelite.launcher.process.runner.ProcessRunnerType;

public class LockServiceTester {

  private static final String pipelineName = UniqueStringGenerator.randomPipelineName();
  private static final String launcherName1 = UniqueStringGenerator.randomLauncherName();
  private static final String launcherName2 = UniqueStringGenerator.randomLauncherName();

  public static void testLauncherLocks(LockService service, Duration lockDuration) {
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
        .isAfterOrEqualTo(ZonedDateTime.now().plus(lockDuration).minus(Duration.ofSeconds(10)));

    assertThat(launcherLock2.getLauncherId()).isGreaterThan(0);
    assertThat(launcherLock2.getLauncherName()).isEqualTo(launcherName2);
    assertThat(launcherLock2.getExpiry())
        .isAfterOrEqualTo(ZonedDateTime.now().plus(lockDuration).minus(Duration.ofSeconds(10)));

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
}
