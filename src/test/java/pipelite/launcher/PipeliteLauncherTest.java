package pipelite.launcher;

import org.junit.jupiter.api.Test;
import pipelite.TestProcessSource;
import pipelite.UniqueStringGenerator;
import pipelite.configuration.LauncherConfiguration;
import pipelite.entity.ProcessEntity;
import pipelite.lock.PipeliteLocker;
import pipelite.process.Process;
import pipelite.process.ProcessFactory;
import pipelite.process.ProcessSource;
import pipelite.service.*;

import static pipelite.launcher.PipeliteLauncher.isRunProcess;
import static pipelite.launcher.PipeliteLauncher.isQueueProcesses;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;
import static org.mockito.internal.verification.VerificationModeFactory.times;

public class PipeliteLauncherTest {

  @Test
  public void testIsQueueProcesses() {
    LocalDateTime now = LocalDateTime.now();
    LocalDateTime later = LocalDateTime.now().plusHours(1);
    int queue = 10;
    int parallelism = 5;
    // Queue is running low and we can refresh the queue now.
    assertThat(isQueueProcesses(queue, queue, later, now, parallelism)).isTrue();
    assertThat(isQueueProcesses(queue - parallelism, queue, later, now, parallelism)).isFalse();
    assertThat(isQueueProcesses(queue - parallelism + 1, queue, later, now, parallelism)).isTrue();
    // Queue is not running low but we must refresh the queue now.
    assertThat(isQueueProcesses(0, queue, now, now, parallelism)).isTrue();
    // Queue is running low but we can't refresh it yet.
    assertThat(isQueueProcesses(queue, queue, later, later, parallelism)).isFalse();
    // Queue is not running low.
    assertThat(isQueueProcesses(0, queue, later, now, parallelism)).isFalse();
  }

  @Test
  public void testIsRunProcess() {
    int queue = 10;
    int parallelism = 5;
    // We can run more processes.
    assertThat(isRunProcess(0, queue, 0, parallelism)).isTrue();
    // Out of processes.
    assertThat(isRunProcess(queue, queue, 0, parallelism)).isFalse();
    // Out of active processes.
    assertThat(isRunProcess(0, queue, parallelism - 1, parallelism)).isTrue();
    assertThat(isRunProcess(0, queue, parallelism, parallelism)).isFalse();
  }

  @Test
  public void testRunProcess() {
    final int processCnt = 100;
    String pipelineName = UniqueStringGenerator.randomPipelineName();

    ProcessFactory processFactory =
        new ProcessFactory() {
          public String getPipelineName() {
            return pipelineName;
          }

          public Process create(String processId) {
            return mock(Process.class);
          }
        };

    PipeliteLauncher launcher =
        spy(
            new PipeliteLauncher(
                new LauncherConfiguration(),
                mock(PipeliteLocker.class),
                processFactory,
                mock(ProcessSource.class),
                mock(ProcessService.class),
                () -> mock(ProcessLauncherPool.class),
                pipelineName));

    List<ProcessEntity> processesEntities =
        Collections.nCopies(processCnt, mock(ProcessEntity.class));
    when(launcher.getActiveProcesses()).thenReturn(processesEntities);

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
    LauncherConfiguration launcherConfiguration = new LauncherConfiguration();
    launcherConfiguration.setProcessQueueMaxRefreshFrequency(refreshFrequency);
    launcherConfiguration.setProcessQueueMinRefreshFrequency(refreshFrequency);

    PipeliteLauncher launcher =
        spy(
            new PipeliteLauncher(
                launcherConfiguration,
                mock(PipeliteLocker.class),
                mock(ProcessFactory.class),
                mock(ProcessSource.class),
                mock(ProcessService.class),
                () -> mock(ProcessLauncherPool.class),
                pipelineName));

    assertThat(launcher.getProcessQueueMaxValidUntil()).isBefore(LocalDateTime.now());
    assertThat(launcher.getProcessQueueMinValidUntil()).isBefore(LocalDateTime.now());

    List<ProcessEntity> processesEntities =
        Collections.nCopies(processCnt, mock(ProcessEntity.class));
    doAnswer(i -> processesEntities).when(launcher).getActiveProcesses();
    doAnswer(i -> processesEntities).when(launcher).getPendingProcesses();

    launcher.startUp();
    launcher.run();

    LocalDateTime plusRefresh = LocalDateTime.now().plus(refreshFrequency);
    LocalDateTime plusBeforeRefresh = LocalDateTime.now().plus(Duration.ofHours(23));
    assertThat(launcher.getProcessQueueMaxValidUntil()).isAfter(plusBeforeRefresh);
    assertThat(launcher.getProcessQueueMinValidUntil()).isAfter(plusBeforeRefresh);
    assertThat(plusRefresh.isAfter(launcher.getProcessQueueMaxValidUntil()));
    assertThat(plusRefresh.isAfter(launcher.getProcessQueueMinValidUntil()));

    verify(launcher, times(1)).run();
    verify(launcher, times(1)).queueProcesses();
    verify(launcher, times(1)).getActiveProcesses();
    verify(launcher, times(1)).getPendingProcesses();
    assertThat(launcher.processQueue.size()).isEqualTo(processCnt * 2);
  }

  @Test
  public void testCreateProcess() {
    final int processCnt = 100;
    String pipelineName = UniqueStringGenerator.randomPipelineName();
    PipeliteLauncher launcher =
        spy(
            new PipeliteLauncher(
                new LauncherConfiguration(),
                mock(PipeliteLocker.class),
                mock(ProcessFactory.class),
                new TestProcessSource(pipelineName, processCnt),
                mock(ProcessService.class),
                () -> mock(ProcessLauncherPool.class),
                pipelineName));

    launcher.startUp();
    launcher.run();

    verify(launcher, times(1)).run();
    verify(launcher, times(1)).createProcesses();
    verify(launcher, times(processCnt)).createProcess(any());
  }
}
