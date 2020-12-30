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
package pipelite.launcher.process.runner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import pipelite.PipeliteTestBeans;
import pipelite.configuration.LauncherConfiguration;
import pipelite.lock.PipeliteLocker;
import pipelite.metrics.PipeliteMetrics;

public class ProcessRunnerPoolServiceTest {

  public static final String LAUNCHER_NAME = "LAUNCHER1";

  @Test
  public void locker() {
    AtomicLong lockCnt = new AtomicLong();
    AtomicLong renewLockCnt = new AtomicLong();
    AtomicLong runCnt = new AtomicLong();
    AtomicLong unlockCnt = new AtomicLong();

    LauncherConfiguration launcherConfiguration = new LauncherConfiguration();

    PipeliteLocker locker = mock(PipeliteLocker.class);
    when(locker.getLauncherName()).thenReturn(LAUNCHER_NAME);
    doAnswer(
            I -> {
              lockCnt.incrementAndGet();
              return null;
            })
        .when(locker)
        .lock();
    doAnswer(
            I -> {
              renewLockCnt.incrementAndGet();
              return null;
            })
        .when(locker)
        .renewLock();
    doAnswer(
            I -> {
              unlockCnt.incrementAndGet();
              return null;
            })
        .when(locker)
        .unlock();

    PipeliteMetrics metrics = PipeliteTestBeans.pipeliteMetrics();

    ProcessRunnerPoolService processRunnerPoolService =
        spy(
            new ProcessRunnerPoolService(
                launcherConfiguration,
                locker,
                LAUNCHER_NAME,
                mock(DefaultProcessRunnerPool.class),
                metrics) {
              @Override
              protected void run() {
                runCnt.incrementAndGet();
              }
            });
    doReturn(true).when(processRunnerPoolService).isActive();

    assertThat(processRunnerPoolService.getLauncherName()).isEqualTo(LAUNCHER_NAME);
    assertThat(processRunnerPoolService.getActiveProcessCount()).isZero();
    assertThat(processRunnerPoolService.getActiveProcessRunners()).isEmpty();

    assertThat(lockCnt.get()).isZero();
    assertThat(renewLockCnt.get()).isZero();
    assertThat(runCnt.get()).isZero();
    assertThat(unlockCnt.get()).isZero();

    processRunnerPoolService.startUp();

    assertThat(lockCnt.get()).isOne();
    assertThat(renewLockCnt.get()).isZero();
    assertThat(runCnt.get()).isZero();
    assertThat(unlockCnt.get()).isZero();

    processRunnerPoolService.runOneIteration();

    assertThat(lockCnt.get()).isOne();
    assertThat(renewLockCnt.get()).isOne();
    assertThat(runCnt.get()).isOne();
    assertThat(unlockCnt.get()).isZero();

    processRunnerPoolService.runOneIteration();

    assertThat(lockCnt.get()).isOne();
    assertThat(renewLockCnt.get()).isEqualTo(2);
    assertThat(runCnt.get()).isEqualTo(2);
    assertThat(unlockCnt.get()).isZero();

    processRunnerPoolService.shutDown();

    assertThat(lockCnt.get()).isOne();
    assertThat(renewLockCnt.get()).isEqualTo(2);
    assertThat(runCnt.get()).isEqualTo(2);
    assertThat(unlockCnt.get()).isOne();
  }
}
