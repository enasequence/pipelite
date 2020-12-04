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

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;
import static org.mockito.internal.verification.VerificationModeFactory.times;

public class PipeliteLauncherTest {

  @Test
  public void isQueueProcesses() {
    LocalDateTime now = LocalDateTime.now();
    LocalDateTime later = LocalDateTime.now().plusHours(1);
    int queueSize = 10;
    int parallelism = 5;
    // Queue is running low and we can refresh the queue now.
    assertThat(PipeliteLauncher.isQueueProcesses(queueSize, queueSize, later, now, parallelism))
        .isTrue();
    assertThat(
            PipeliteLauncher.isQueueProcesses(
                queueSize - parallelism, queueSize, later, now, parallelism))
        .isFalse();
    assertThat(
            PipeliteLauncher.isQueueProcesses(
                queueSize - parallelism + 1, queueSize, later, now, parallelism))
        .isTrue();
    // Queue is not running low but we must refresh the queue now.
    assertThat(PipeliteLauncher.isQueueProcesses(0, queueSize, now, now, parallelism)).isTrue();
    // Queue is running low but we can't refresh it yet.
    assertThat(PipeliteLauncher.isQueueProcesses(queueSize, queueSize, later, later, parallelism))
        .isFalse();
    // Queue is not running low.
    assertThat(PipeliteLauncher.isQueueProcesses(0, queueSize, later, now, parallelism)).isFalse();
  }

  @Test
  public void isRunProcess() {
    int queueSize = 10;
    int parallelism = 5;
    // We can run more processes.
    assertThat(PipeliteLauncher.isRunProcess(0, queueSize, 0, parallelism)).isTrue();
    // Out of processes.
    assertThat(PipeliteLauncher.isRunProcess(queueSize, queueSize, 0, parallelism)).isFalse();
    // Out of active processes.
    assertThat(PipeliteLauncher.isRunProcess(0, queueSize, parallelism - 1, parallelism)).isTrue();
    assertThat(PipeliteLauncher.isRunProcess(0, queueSize, parallelism, parallelism)).isFalse();
  }

  @Test
  public void runProcess() {
    final int processCnt = 100;
    String pipelineName = UniqueStringGenerator.randomPipelineName();

    LauncherConfiguration launcherConfiguration = new LauncherConfiguration();
    ProcessFactory processFactory =
        new ProcessFactory() {
          @Override
          public String getPipelineName() {
            return pipelineName;
          }

          @Override
          public Process create(String processId) {
            Process process = mock(Process.class);
            when(process.getProcessId()).thenReturn("PROCESS_ID");
            return process;
          }
        };
    ProcessSource processSource = mock(ProcessSource.class);
    ProcessService processService = mock(ProcessService.class);
    ProcessLauncherPool processLauncherPool = mock(ProcessLauncherPool.class);
    PipeliteLocker pipeliteLocker = mock(PipeliteLocker.class);

    PipeliteLauncher launcher =
        spy(
            new PipeliteLauncher(
                launcherConfiguration,
                pipeliteLocker,
                processFactory,
                processSource,
                processService,
                () -> processLauncherPool,
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
  public void queueProcesses() {
    final int processCnt = 100;
    String pipelineName = UniqueStringGenerator.randomPipelineName();

    Duration refreshFrequency = Duration.ofDays(1);
    LauncherConfiguration launcherConfiguration = new LauncherConfiguration();
    launcherConfiguration.setProcessQueueMaxRefreshFrequency(refreshFrequency);
    launcherConfiguration.setProcessQueueMinRefreshFrequency(refreshFrequency);

    ProcessFactory processFactory = mock(ProcessFactory.class);
    ProcessSource processSource = mock(ProcessSource.class);
    ProcessService processService = mock(ProcessService.class);
    ProcessLauncherPool processLauncherPool = mock(ProcessLauncherPool.class);
    PipeliteLocker pipeliteLocker = mock(PipeliteLocker.class);

    PipeliteLauncher launcher =
        spy(
            new PipeliteLauncher(
                launcherConfiguration,
                pipeliteLocker,
                processFactory,
                processSource,
                processService,
                () -> processLauncherPool,
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
  public void createProcess() {
    final int processCnt = 100;
    String pipelineName = UniqueStringGenerator.randomPipelineName();

    LauncherConfiguration launcherConfiguration = new LauncherConfiguration();
    ProcessFactory processFactory = mock(ProcessFactory.class);
    ProcessSource processSource = new TestProcessSource(pipelineName, processCnt);
    ProcessService processService = mock(ProcessService.class);
    ProcessLauncherPool processLauncherPool = mock(ProcessLauncherPool.class);
    PipeliteLocker pipeliteLocker = mock(PipeliteLocker.class);
    when(pipeliteLocker.getLauncherName()).thenReturn("LAUNCHER_NAME");

    PipeliteLauncher launcher =
        spy(
            new PipeliteLauncher(
                launcherConfiguration,
                pipeliteLocker,
                processFactory,
                processSource,
                processService,
                () -> processLauncherPool,
                pipelineName));

    launcher.startUp();
    launcher.run();

    verify(launcher, times(1)).run();
    verify(launcher, times(1)).createProcesses();
    verify(launcher, times(processCnt)).createProcess(any());
  }
}
