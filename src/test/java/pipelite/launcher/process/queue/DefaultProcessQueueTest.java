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
package pipelite.launcher.process.queue;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;
import static org.mockito.internal.verification.VerificationModeFactory.times;
import static pipelite.launcher.process.queue.DefaultProcessQueue.isFillQueue;

import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;
import pipelite.UniqueStringGenerator;
import pipelite.configuration.LauncherConfiguration;
import pipelite.entity.ProcessEntity;
import pipelite.service.ProcessService;

public class DefaultProcessQueueTest {

  @Test
  public void fillQueue() {
    ZonedDateTime now = ZonedDateTime.now();
    ZonedDateTime later = ZonedDateTime.now().plusHours(1);
    int len = 10; // queue length
    int par = 5; // parallelism
    // Queue is running low and we can refresh the queue now.
    assertThat(isFillQueue(len, len, later, now, par)).isTrue();
    assertThat(isFillQueue(len - par, len, later, now, par)).isFalse();
    assertThat(isFillQueue(len - par + 1, len, later, now, par)).isTrue();
    // Queue is not running low but we must refresh the queue now.
    assertThat(isFillQueue(0, len, now, now, par)).isTrue();
    // Queue is running low but we can't refresh it yet.
    assertThat(isFillQueue(len, len, later, later, par)).isFalse();
    // Queue is not running low.
    assertThat(isFillQueue(0, len, later, now, par)).isFalse();
  }

  @Test
  public void lifecycle() {
    final int getActiveProcessCnt = 100;
    final int getPendingProcessCnt = 50;
    final int processQueueMaxSize = 150;
    final int processParallelism = 10;
    String pipelineName = UniqueStringGenerator.randomPipelineName();
    Duration refreshFrequency = Duration.ofDays(1);
    LauncherConfiguration launcherConfiguration = new LauncherConfiguration();
    launcherConfiguration.setProcessQueueMaxRefreshFrequency(refreshFrequency);
    launcherConfiguration.setProcessQueueMinRefreshFrequency(refreshFrequency);
    launcherConfiguration.setProcessQueueMaxSize(processQueueMaxSize);
    launcherConfiguration.setProcessParallelism(processParallelism);

    ProcessService processService = mock(ProcessService.class);

    List<ProcessEntity> activeEntities =
        Collections.nCopies(getActiveProcessCnt, mock(ProcessEntity.class));
    List<ProcessEntity> pendingEntities =
        Collections.nCopies(getPendingProcessCnt, mock(ProcessEntity.class));
    doReturn(activeEntities).when(processService).getActiveProcesses(any(), any(), eq(150));
    doReturn(pendingEntities).when(processService).getPendingProcesses(any(), eq(50));

    DefaultProcessQueue queue =
        spy(new DefaultProcessQueue(launcherConfiguration, processService, pipelineName));

    assertThat(queue.isFillQueue()).isTrue();
    assertThat(queue.isAvailableProcesses(0)).isFalse();
    ZonedDateTime processQueueMaxValidUntil = queue.getProcessQueueMaxValidUntil();
    ZonedDateTime processQueueMinValidUntil = queue.getProcessQueueMinValidUntil();
    assertThat(processQueueMaxValidUntil).isBeforeOrEqualTo(ZonedDateTime.now());
    assertThat(processQueueMinValidUntil).isBeforeOrEqualTo(ZonedDateTime.now());

    // Queue processes.
    assertThat(queue.fillQueue()).isEqualTo(processQueueMaxSize);

    verify(queue, times(1)).fillQueue();
    verify(queue, times(1)).getActiveProcesses();
    verify(queue, times(1)).getPendingProcesses();

    assertThat(queue.isFillQueue()).isFalse();
    assertThat(queue.isAvailableProcesses(0)).isTrue();
    assertThat(queue.isAvailableProcesses(processParallelism - 1)).isTrue();
    assertThat(queue.isAvailableProcesses(processParallelism)).isFalse();
    assertThat(queue.getProcessQueueMaxValidUntil()).isAfter(processQueueMaxValidUntil);
    assertThat(queue.getProcessQueueMinValidUntil()).isAfter(processQueueMinValidUntil);

    while (queue.isAvailableProcesses(0)) {
      assertThat(queue.nextAvailableProcess()).isNotNull();
    }
    assertThat(queue.isAvailableProcesses(0)).isFalse();
  }
}
