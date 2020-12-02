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
package pipelite.lock;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.*;

import org.junit.jupiter.api.Test;
import pipelite.entity.LauncherLockEntity;
import pipelite.service.LockService;

public class PipeliteLockerTest {

  private static final String LAUNCHER_NAME = "LAUNCHER1";

  @Test
  public void lifecycle() {
    LockService lockService = mock(LockService.class);
    LauncherLockEntity lock = new LauncherLockEntity();
    when(lockService.lockLauncher(LAUNCHER_NAME)).thenAnswer((launcherName) -> lock);
    when(lockService.relockLauncher(lock)).thenAnswer((launcherName) -> true);
    PipeliteLocker locker = new PipeliteLocker(lockService, LAUNCHER_NAME);
    locker.lock();
    assertThat(locker.getLock()).isSameAs(lock);
    locker.renewLock();
    assertThat(locker.getLock()).isSameAs(lock);
    locker.renewLock();
    assertThat(locker.getLock()).isSameAs(lock);
    locker.unlock();
    assertThat(locker.getLock()).isSameAs(lock);
    locker.lock();
    assertThat(locker.getLock()).isSameAs(lock);
  }

  @Test
  public void lockThrows() {
    LockService lockService = mock(LockService.class);
    RuntimeException ex = new RuntimeException("Expected exception");
    when(lockService.lockLauncher(LAUNCHER_NAME)).thenThrow(ex);
    PipeliteLocker locker = new PipeliteLocker(lockService, LAUNCHER_NAME);
    assertThatThrownBy(() -> locker.lock()).isSameAs(ex);
    assertThat(locker.getLock()).isNull();
  }

  @Test
  public void renewLockThrows() {
    LockService lockService = mock(LockService.class);
    RuntimeException ex = new RuntimeException("Expected exception");
    LauncherLockEntity lock = new LauncherLockEntity();
    when(lockService.lockLauncher(LAUNCHER_NAME)).thenAnswer((launcherName) -> lock);
    when(lockService.relockLauncher(lock)).thenThrow(ex);
    PipeliteLocker locker = new PipeliteLocker(lockService, LAUNCHER_NAME);
    locker.lock();
    assertThat(locker.getLock()).isSameAs(lock);
    assertThatThrownBy(() -> locker.renewLock()).isSameAs(ex);
    assertThat(locker.getLock()).isSameAs(lock);
  }

  @Test
  public void unlockThrows() {
    LockService lockService = mock(LockService.class);
    RuntimeException ex = new RuntimeException("Expected exception");
    LauncherLockEntity lock = new LauncherLockEntity();
    when(lockService.lockLauncher(LAUNCHER_NAME)).thenAnswer((launcherName) -> lock);
    when(lockService.relockLauncher(lock)).thenAnswer((launcherName) -> true);
    doThrow(ex).when(lockService).unlockLauncher(lock);
    PipeliteLocker locker = new PipeliteLocker(lockService, LAUNCHER_NAME);
    locker.lock();
    assertThat(locker.getLock()).isSameAs(lock);
    locker.renewLock();
    assertThat(locker.getLock()).isSameAs(lock);
    locker.renewLock();
    assertThat(locker.getLock()).isSameAs(lock);
    locker.unlock();
    assertThat(locker.getLock()).isSameAs(lock);
    locker.lock();
    assertThat(locker.getLock()).isSameAs(lock);
  }
}
