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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import org.junit.jupiter.api.Test;
import pipelite.configuration.LauncherConfiguration;
import pipelite.lock.PipeliteLocker;
import pipelite.process.Process;
import pipelite.process.ProcessState;

public class ProcessLauncherServiceTest {

  public static final String LAUNCHER_NAME = "LAUNCHER1";

  private Supplier<ProcessLauncher> processLauncherSupplier(ProcessState state) {
    return () -> {
      ProcessLauncher processLauncher = mock(ProcessLauncher.class);
      doAnswer(
              i -> {
                Process process = (Process) i.getArguments()[1];
                process.getProcessEntity().endExecution(state);
                return null;
              })
          .when(processLauncher)
          .run(any(), any());
      return processLauncher;
    };
  }

  @Test
  public void lifecycle() {
    AtomicLong lockCallCnt = new AtomicLong();
    AtomicLong renewLockCallCnt = new AtomicLong();
    AtomicLong runCallCnt = new AtomicLong();
    AtomicLong unlockCallCnt = new AtomicLong();

    LauncherConfiguration launcherConfiguration = new LauncherConfiguration();
    PipeliteLocker locker = mock(PipeliteLocker.class);
    when(locker.getLauncherName()).thenReturn(LAUNCHER_NAME);

    doAnswer(
            a -> {
              lockCallCnt.incrementAndGet();
              return null;
            })
        .when(locker)
        .lock();
    doAnswer(
            a -> {
              renewLockCallCnt.incrementAndGet();
              return null;
            })
        .when(locker)
        .renewLock();
    doAnswer(
            a -> {
              unlockCallCnt.incrementAndGet();
              return null;
            })
        .when(locker)
        .unlock();

    ProcessLauncherService processLauncherService =
        spy(
            new ProcessLauncherService(
                launcherConfiguration,
                locker,
                LAUNCHER_NAME,
                () -> new ProcessLauncherPool(processLauncherSupplier(ProcessState.COMPLETED))) {
              @Override
              protected void run() {
                runCallCnt.incrementAndGet();
              }
            });
    doReturn(true).when(processLauncherService).isActive();

    assertThat(processLauncherService.getLauncherName()).isEqualTo(LAUNCHER_NAME);
    assertThat(processLauncherService.activeProcessCount()).isZero();
    assertThat(processLauncherService.getProcessLaunchers()).isEmpty();

    assertThat(lockCallCnt.get()).isZero();
    assertThat(renewLockCallCnt.get()).isZero();
    assertThat(runCallCnt.get()).isZero();
    assertThat(unlockCallCnt.get()).isZero();

    processLauncherService.startUp();

    assertThat(lockCallCnt.get()).isOne();
    assertThat(renewLockCallCnt.get()).isZero();
    assertThat(runCallCnt.get()).isZero();
    assertThat(unlockCallCnt.get()).isZero();

    processLauncherService.runOneIteration();

    assertThat(lockCallCnt.get()).isOne();
    assertThat(renewLockCallCnt.get()).isOne();
    assertThat(runCallCnt.get()).isOne();
    assertThat(unlockCallCnt.get()).isZero();

    processLauncherService.runOneIteration();

    assertThat(lockCallCnt.get()).isOne();
    assertThat(renewLockCallCnt.get()).isEqualTo(2);
    assertThat(runCallCnt.get()).isEqualTo(2);
    assertThat(unlockCallCnt.get()).isZero();

    processLauncherService.shutDown();

    assertThat(lockCallCnt.get()).isOne();
    assertThat(renewLockCallCnt.get()).isEqualTo(2);
    assertThat(runCallCnt.get()).isEqualTo(2);
    assertThat(unlockCallCnt.get()).isOne();
  }
}
