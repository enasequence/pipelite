package pipelite.launcher;

import org.junit.jupiter.api.Test;
import pipelite.entity.LauncherLockEntity;
import pipelite.entity.ProcessEntity;
import pipelite.process.Process;
import pipelite.process.ProcessState;
import pipelite.process.builder.ProcessBuilder;
import pipelite.service.LockService;
import pipelite.stage.StageExecutionResultType;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.mockito.internal.verification.VerificationModeFactory.times;

public class ProcessLauncherPoolTest {

  private static final int PROCESS_CNT = 1000;
  public static final String PIPELINE_NAME = "PIPELINE1";

  @Test
  public void testSuccess() throws InterruptedException {
    LockService lockService = mock(LockService.class);
    LauncherLockEntity lock = mock(LauncherLockEntity.class);

    when(lockService.lockProcess(any(), any(), any())).thenReturn(true);

    ProcessLauncherPool pool =
        new ProcessLauncherPool(
            lockService,
            (pipelineName, process) -> {
              process.getProcessEntity().endExecution(ProcessState.COMPLETED);
              return mock(ProcessLauncher.class);
            },
            lock);

    ProcessLauncherStats stats = new ProcessLauncherStats();

    for (int i = 0; i < PROCESS_CNT; i++) {
      Process process =
          new ProcessBuilder("PROCESS" + i)
              .execute("STAGE1")
              .withEmptySyncExecutor(StageExecutionResultType.SUCCESS)
              .build();
      ProcessEntity processEntity = new ProcessEntity();
      process.setProcessEntity(processEntity);
      pool.run(PIPELINE_NAME, process, (p, r) -> stats.add(p, r));
    }

    pool.shutDown();

    assertThat(stats.getProcessExecutionCount(ProcessState.COMPLETED)).isEqualTo(PROCESS_CNT);
    assertThat(stats.getProcessExecutionCount(ProcessState.FAILED)).isZero();
    assertThat(stats.getProcessExceptionCount()).isZero();
    verify(lockService, times(PROCESS_CNT)).lockProcess(eq(lock), eq(PIPELINE_NAME), any());
    verify(lockService, times(PROCESS_CNT)).unlockProcess(eq(lock), eq(PIPELINE_NAME), any());
    assertThat(pool.size()).isZero();
    assertThat(pool.get().size()).isZero();
  }

  @Test
  public void testFailed() throws InterruptedException {
    LockService lockService = mock(LockService.class);
    LauncherLockEntity lock = mock(LauncherLockEntity.class);

    when(lockService.lockProcess(any(), any(), any())).thenReturn(true);

    ProcessLauncherPool pool =
        new ProcessLauncherPool(
            lockService,
            (pipelineName, process) -> {
              process.getProcessEntity().endExecution(ProcessState.FAILED);
              return mock(ProcessLauncher.class);
            },
            lock);

    ProcessLauncherStats stats = new ProcessLauncherStats();

    for (int i = 0; i < PROCESS_CNT; i++) {
      Process process =
          new ProcessBuilder("PROCESS" + i)
              .execute("STAGE1")
              .withEmptySyncExecutor(StageExecutionResultType.SUCCESS)
              .build();
      ProcessEntity processEntity = new ProcessEntity();
      process.setProcessEntity(processEntity);
      pool.run(PIPELINE_NAME, process, (p, r) -> stats.add(p, r));
    }

    pool.shutDown();

    assertThat(stats.getProcessExecutionCount(ProcessState.COMPLETED)).isZero();
    assertThat(stats.getProcessExecutionCount(ProcessState.FAILED)).isEqualTo(PROCESS_CNT);
    assertThat(stats.getProcessExceptionCount()).isZero();
    verify(lockService, times(PROCESS_CNT)).lockProcess(eq(lock), eq(PIPELINE_NAME), any());
    verify(lockService, times(PROCESS_CNT)).unlockProcess(eq(lock), eq(PIPELINE_NAME), any());
    assertThat(pool.size()).isZero();
    assertThat(pool.get().size()).isZero();
  }

  @Test
  public void testException() throws InterruptedException {
    LockService lockService = mock(LockService.class);
    LauncherLockEntity lock = mock(LauncherLockEntity.class);

    when(lockService.lockProcess(any(), any(), any())).thenReturn(true);

    ProcessLauncherPool pool =
        new ProcessLauncherPool(
            lockService,
            (pipelineName, process) -> {
              process.getProcessEntity().endExecution(ProcessState.FAILED);
              ProcessLauncher launcher = mock(ProcessLauncher.class);
              doThrow(new RuntimeException()).when(launcher).run();
              return launcher;
            },
            lock);

    ProcessLauncherStats stats = new ProcessLauncherStats();

    for (int i = 0; i < PROCESS_CNT; i++) {
      Process process =
          new ProcessBuilder("PROCESS" + i)
              .execute("STAGE1")
              .withEmptySyncExecutor(StageExecutionResultType.SUCCESS)
              .build();
      ProcessEntity processEntity = new ProcessEntity();
      process.setProcessEntity(processEntity);
      pool.run(PIPELINE_NAME, process, (p, r) -> stats.add(p, r));
    }

    pool.shutDown();

    assertThat(stats.getProcessExecutionCount(ProcessState.COMPLETED)).isZero();
    assertThat(stats.getProcessExecutionCount(ProcessState.FAILED)).isZero();
    assertThat(stats.getProcessExceptionCount()).isEqualTo(PROCESS_CNT);
    verify(lockService, times(PROCESS_CNT)).lockProcess(eq(lock), eq(PIPELINE_NAME), any());
    verify(lockService, times(PROCESS_CNT)).unlockProcess(eq(lock), eq(PIPELINE_NAME), any());
    assertThat(pool.size()).isZero();
    assertThat(pool.get().size()).isZero();
  }
}
