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
import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ForkJoinPool;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import pipelite.UniqueStringGenerator;
import pipelite.configuration.LauncherConfiguration;
import pipelite.configuration.WebConfiguration;
import pipelite.entity.ProcessEntity;
import pipelite.launcher.process.creator.ProcessCreator;
import pipelite.launcher.process.queue.DefaultProcessQueue;
import pipelite.launcher.process.runner.DefaultProcessRunnerPool;
import pipelite.lock.PipeliteLocker;
import pipelite.process.Process;
import pipelite.process.ProcessFactory;
import pipelite.service.*;

public class PipeliteLauncherTest {

  @Test
  public void runProcess() {
    final int processCnt = 100;
    String pipelineName = UniqueStringGenerator.randomPipelineName();
    WebConfiguration webConfiguration = new WebConfiguration();
    LauncherConfiguration launcherConfiguration = new LauncherConfiguration();
    int processParallelism = ForkJoinPool.getCommonPoolParallelism();

    ProcessFactory processFactory =
        new ProcessFactory() {
          @Override
          public String getPipelineName() {
            return pipelineName;
          }

          @Override
          public int getProcessParallelism() {
            return processParallelism;
          }

          @Override
          public Process create(String processId) {
            return mock(Process.class);
          }
        };

    DefaultProcessQueue queue =
        spy(
            new DefaultProcessQueue(
                webConfiguration,
                launcherConfiguration,
                mock(ProcessService.class),
                pipelineName,
                processFactory.getProcessParallelism()));

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
                () -> mock(DefaultProcessRunnerPool.class),
                new SimpleMeterRegistry()));

    launcher.startUp();
    launcher.run();

    verify(launcher, times(1)).run();
    verify(launcher, times(processCnt)).runProcess(any());
  }

  @Test
  public void testQueueProcesses() {
    final int processCnt = 100;
    String pipelineName = UniqueStringGenerator.randomPipelineName();
    Duration refreshFrequency = Duration.ofDays(1);
    WebConfiguration webConfiguration = new WebConfiguration();
    LauncherConfiguration launcherConfiguration = new LauncherConfiguration();
    launcherConfiguration.setProcessQueueMaxRefreshFrequency(refreshFrequency);
    launcherConfiguration.setProcessQueueMinRefreshFrequency(refreshFrequency);
    int processParallelism = ForkJoinPool.getCommonPoolParallelism();

    DefaultProcessQueue queue =
        spy(
            new DefaultProcessQueue(
                webConfiguration,
                launcherConfiguration,
                mock(ProcessService.class),
                pipelineName,
                processParallelism));

    assertThat(queue.getProcessQueueMaxValidUntil()).isBefore(ZonedDateTime.now());
    assertThat(queue.getProcessQueueMinValidUntil()).isBefore(ZonedDateTime.now());

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
                () -> mock(DefaultProcessRunnerPool.class),
                new SimpleMeterRegistry()));

    launcher.startUp();
    launcher.run();

    ZonedDateTime plusRefresh = ZonedDateTime.now().plus(refreshFrequency);
    ZonedDateTime plusBeforeRefresh = ZonedDateTime.now().plus(Duration.ofHours(23));
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
    String pipelineName = UniqueStringGenerator.randomPipelineName();
    WebConfiguration webConfiguration = new WebConfiguration();
    LauncherConfiguration launcherConfiguration = new LauncherConfiguration();
    launcherConfiguration.setProcessCreateMaxSize(100);
    int processParallelism = ForkJoinPool.getCommonPoolParallelism();

    ProcessCreator processCreator = mock(ProcessCreator.class);
    DefaultProcessQueue queue =
        spy(
            new DefaultProcessQueue(
                webConfiguration,
                launcherConfiguration,
                mock(ProcessService.class),
                pipelineName,
                processParallelism));
    when(queue.isFillQueue()).thenReturn(true);

    PipeliteLauncher launcher =
        spy(
            new PipeliteLauncher(
                launcherConfiguration,
                mock(PipeliteLocker.class),
                mock(ProcessFactory.class),
                processCreator,
                queue,
                () -> mock(DefaultProcessRunnerPool.class),
                new SimpleMeterRegistry()));

    launcher.startUp();
    launcher.run();

    verify(launcher, times(1)).run();
    verify(processCreator, times(1)).createProcesses(processCnt);
  }
}
