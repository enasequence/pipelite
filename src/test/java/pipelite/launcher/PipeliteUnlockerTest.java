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
package pipelite.launcher;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.transaction.annotation.Transactional;
import pipelite.PipeliteTestConfiguration;
import pipelite.UniqueStringGenerator;
import pipelite.configuration.LauncherConfiguration;
import pipelite.entity.LauncherLockEntity;
import pipelite.repository.LauncherLockRepository;
import pipelite.repository.ProcessLockRepository;
import pipelite.service.LockService;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest(
    webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
    classes = PipeliteTestConfiguration.class)
@ActiveProfiles(value = {"hsql-test", "test"})
@Transactional
public class PipeliteUnlockerTest {

  @Autowired LauncherLockRepository launcherLockRepository;
  @Autowired ProcessLockRepository processLockRepository;

  private static final String pipelineName = UniqueStringGenerator.randomPipelineName();
  private static final String launcherName = UniqueStringGenerator.randomLauncherName();

  @Test
  public void expired() {
    LauncherConfiguration launcherConfiguration = new LauncherConfiguration();
    launcherConfiguration.setPipelineLockDuration(Duration.ofMillis(0));
    launcherConfiguration.setPipelineUnlockFrequency(Duration.ofMillis(1000));

    LockService lockService =
        new LockService(launcherConfiguration, launcherLockRepository, processLockRepository);

    lockService
        .getLauncherLocksByLauncherName(launcherName)
        .forEach(s -> lockService.unlockLauncher(s));
    lockService.unlockProcessesByPipelineName(pipelineName);
    LauncherLockEntity launcherLock = lockService.lockLauncher(launcherName);
    assertThat(lockService.getProcessLocks(launcherLock)).isEmpty();

    assertTrue(lockService.lockProcess(launcherLock, pipelineName, "1"));
    assertTrue(lockService.lockProcess(launcherLock, pipelineName, "2"));
    assertTrue(lockService.lockProcess(launcherLock, pipelineName, "3"));
    assertTrue(lockService.isProcessLocked(pipelineName, "1"));
    assertTrue(lockService.isProcessLocked(pipelineName, "2"));
    assertTrue(lockService.isProcessLocked(pipelineName, "3"));

    PipeliteUnlocker unlocker = new PipeliteUnlocker(launcherConfiguration, lockService);
    unlocker.stopAsync().awaitTerminated();
    unlocker.removeExpiredLocks();

    assertThat(lockService.getLauncherLocksByLauncherName(launcherName).size()).isZero();
    assertFalse(lockService.isProcessLocked(pipelineName, "1"));
    assertFalse(lockService.isProcessLocked(pipelineName, "2"));
    assertFalse(lockService.isProcessLocked(pipelineName, "3"));
  }

  @Test
  public void notExpired() {
    LauncherConfiguration launcherConfiguration = new LauncherConfiguration();
    launcherConfiguration.setPipelineLockDuration(Duration.ofHours(1));
    launcherConfiguration.setPipelineUnlockFrequency(Duration.ofMillis(1000));

    LockService lockService =
        new LockService(launcherConfiguration, launcherLockRepository, processLockRepository);

    lockService
        .getLauncherLocksByLauncherName(launcherName)
        .forEach(s -> lockService.unlockLauncher(s));
    lockService.unlockProcessesByPipelineName(pipelineName);
    LauncherLockEntity launcherLock = lockService.lockLauncher(launcherName);
    assertThat(lockService.getProcessLocks(launcherLock)).isEmpty();

    assertTrue(lockService.lockProcess(launcherLock, pipelineName, "1"));
    assertTrue(lockService.lockProcess(launcherLock, pipelineName, "2"));
    assertTrue(lockService.lockProcess(launcherLock, pipelineName, "3"));
    assertTrue(lockService.isProcessLocked(pipelineName, "1"));
    assertTrue(lockService.isProcessLocked(pipelineName, "2"));
    assertTrue(lockService.isProcessLocked(pipelineName, "3"));

    PipeliteUnlocker unlocker = new PipeliteUnlocker(launcherConfiguration, lockService);
    unlocker.stopAsync().awaitTerminated();
    unlocker.removeExpiredLocks();

    assertThat(lockService.getLauncherLocksByLauncherName(launcherName).size()).isOne();
    assertTrue(lockService.isProcessLocked(pipelineName, "1"));
    assertTrue(lockService.isProcessLocked(pipelineName, "2"));
    assertTrue(lockService.isProcessLocked(pipelineName, "3"));
  }
}
