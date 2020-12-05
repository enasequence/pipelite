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
import static org.mockito.internal.verification.VerificationModeFactory.times;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;
import pipelite.UniqueStringGenerator;
import pipelite.configuration.LauncherConfiguration;
import pipelite.entity.ProcessEntity;
import pipelite.lock.PipeliteLocker;
import pipelite.process.Process;
import pipelite.process.ProcessFactory;
import pipelite.service.*;

public class PipeliteLauncherTest {

  @Test
  public void runProcess() {
    final int processCnt = 100;
    String launcherName = UniqueStringGenerator.randomLauncherName();
    String pipelineName = UniqueStringGenerator.randomPipelineName();
    LauncherConfiguration launcherConfiguration = new LauncherConfiguration();

    ProcessFactory processFactory =
        new ProcessFactory() {
          public String getPipelineName() {
            return pipelineName;
          }

          public Process create(String processId) {
            return mock(Process.class);
          }
        };

    ProcessQueue queue =
        spy(
            new ProcessQueue(
                launcherConfiguration, mock(ProcessService.class), launcherName, pipelineName));

    List<ProcessEntity> processesEntities =
        Collections.nCopies(processCnt, mock(ProcessEntity.class));
    doReturn(processesEntities).when(queue).getActiveProcesses();

    PipeliteLauncher launcher =
        spy(
            new PipeliteLauncher(
                launcherConfiguration,
                mock(PipeliteLocker.class),
                processFactory,
                mock(ProcessCreator.class),
                queue,
                () -> mock(ProcessLauncherPool.class),
                launcherName,
                pipelineName));

    launcher.startUp();
    launcher.run();

    verify(launcher, times(1)).run();
    verify(launcher, times(processCnt)).runProcess(any());
  }

  @Test
  public void testQueueProcesses() {
    final int processCnt = 100;
    String launcherName = UniqueStringGenerator.randomLauncherName();
    String pipelineName = UniqueStringGenerator.randomPipelineName();
    Duration refreshFrequency = Duration.ofDays(1);
    LauncherConfiguration launcherConfiguration = new LauncherConfiguration();
    launcherConfiguration.setProcessQueueMaxRefreshFrequency(refreshFrequency);
    launcherConfiguration.setProcessQueueMinRefreshFrequency(refreshFrequency);

    ProcessQueue queue =
        spy(
            new ProcessQueue(
                launcherConfiguration, mock(ProcessService.class), launcherName, pipelineName));

    assertThat(queue.getProcessQueueMaxValidUntil()).isBefore(LocalDateTime.now());
    assertThat(queue.getProcessQueueMinValidUntil()).isBefore(LocalDateTime.now());

    List<ProcessEntity> processesEntities =
        Collections.nCopies(processCnt, mock(ProcessEntity.class));
    doReturn(processesEntities).when(queue).getActiveProcesses();
    doReturn(processesEntities).when(queue).getPendingProcesses();

    PipeliteLauncher launcher =
        spy(
            new PipeliteLauncher(
                launcherConfiguration,
                mock(PipeliteLocker.class),
                mock(ProcessFactory.class),
                mock(ProcessCreator.class),
                queue,
                () -> mock(ProcessLauncherPool.class),
                launcherName,
                pipelineName));

    launcher.startUp();
    launcher.run();

    LocalDateTime plusRefresh = LocalDateTime.now().plus(refreshFrequency);
    LocalDateTime plusBeforeRefresh = LocalDateTime.now().plus(Duration.ofHours(23));
    assertThat(queue.getProcessQueueMaxValidUntil()).isAfter(plusBeforeRefresh);
    assertThat(queue.getProcessQueueMinValidUntil()).isAfter(plusBeforeRefresh);
    assertThat(plusRefresh.isAfter(queue.getProcessQueueMaxValidUntil()));
    assertThat(plusRefresh.isAfter(queue.getProcessQueueMinValidUntil()));

    verify(launcher, times(1)).run();
    verify(queue, times(1)).fillQueue();
    verify(queue, times(1)).getActiveProcesses();
    verify(queue, times(1)).getPendingProcesses();
  }

  @Test
  public void testCreateProcess() {
    final int processCnt = 100;
    String launcherName = UniqueStringGenerator.randomLauncherName();
    String pipelineName = UniqueStringGenerator.randomPipelineName();
    LauncherConfiguration launcherConfiguration = new LauncherConfiguration();
    launcherConfiguration.setProcessCreateMaxSize(100);

    ProcessCreator processCreator = mock(ProcessCreator.class);
    ProcessQueue queue =
        spy(
            new ProcessQueue(
                launcherConfiguration, mock(ProcessService.class), launcherName, pipelineName));
    when(queue.isFillQueue()).thenReturn(true);

    PipeliteLauncher launcher =
        spy(
            new PipeliteLauncher(
                launcherConfiguration,
                mock(PipeliteLocker.class),
                mock(ProcessFactory.class),
                processCreator,
                queue,
                () -> mock(ProcessLauncherPool.class),
                launcherName,
                pipelineName));

    launcher.startUp();
    launcher.run();

    verify(launcher, times(1)).run();
    verify(processCreator, times(1)).createProcesses(processCnt);
  }
}
