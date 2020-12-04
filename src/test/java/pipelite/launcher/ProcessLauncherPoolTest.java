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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.mockito.internal.verification.VerificationModeFactory.times;

import java.util.function.Supplier;
import org.junit.jupiter.api.Test;
import pipelite.entity.ProcessEntity;
import pipelite.lock.PipeliteLocker;
import pipelite.process.Process;
import pipelite.process.ProcessState;
import pipelite.process.builder.ProcessBuilder;
import pipelite.stage.StageExecutionResultType;

public class ProcessLauncherPoolTest {

  private static final int PROCESS_CNT = 1000;
  public static final String PIPELINE_NAME = "PIPELINE1";

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
  public void testSuccess() {
    PipeliteLocker locker = mock(PipeliteLocker.class);
    when(locker.lockProcess(any(), any())).thenReturn(true);

    ProcessLauncherPool pool =
        new ProcessLauncherPool(processLauncherSupplier(ProcessState.COMPLETED));

    ProcessLauncherStats stats = new ProcessLauncherStats();

    for (int i = 0; i < PROCESS_CNT; i++) {
      Process process =
          new ProcessBuilder("PROCESS" + i)
              .execute("STAGE1")
              .withEmptySyncExecutor(StageExecutionResultType.SUCCESS)
              .build();
      ProcessEntity processEntity = new ProcessEntity();
      process.setProcessEntity(processEntity);
      pool.run(PIPELINE_NAME, process, locker, (p, r) -> stats.add(p, r));
    }

    pool.shutDown();

    assertThat(stats.getProcessExecutionCount(ProcessState.COMPLETED)).isEqualTo(PROCESS_CNT);
    assertThat(stats.getProcessExecutionCount(ProcessState.FAILED)).isZero();
    assertThat(stats.getProcessExceptionCount()).isZero();
    verify(locker, times(PROCESS_CNT)).lockProcess(eq(PIPELINE_NAME), any());
    verify(locker, times(PROCESS_CNT)).unlockProcess(eq(PIPELINE_NAME), any());
    assertThat(pool.activeProcessCount()).isZero();
    assertThat(pool.get().size()).isZero();
  }

  @Test
  public void testFailed() {
    PipeliteLocker locker = mock(PipeliteLocker.class);
    when(locker.lockProcess(any(), any())).thenReturn(true);

    ProcessLauncherPool pool =
        new ProcessLauncherPool(processLauncherSupplier(ProcessState.FAILED));

    ProcessLauncherStats stats = new ProcessLauncherStats();

    for (int i = 0; i < PROCESS_CNT; i++) {
      Process process =
          new ProcessBuilder("PROCESS" + i)
              .execute("STAGE1")
              .withEmptySyncExecutor(StageExecutionResultType.SUCCESS)
              .build();
      ProcessEntity processEntity = new ProcessEntity();
      process.setProcessEntity(processEntity);
      pool.run(PIPELINE_NAME, process, locker, (p, r) -> stats.add(p, r));
    }

    pool.shutDown();

    assertThat(stats.getProcessExecutionCount(ProcessState.COMPLETED)).isZero();
    assertThat(stats.getProcessExecutionCount(ProcessState.FAILED)).isEqualTo(PROCESS_CNT);
    assertThat(stats.getProcessExceptionCount()).isZero();
    verify(locker, times(PROCESS_CNT)).lockProcess(eq(PIPELINE_NAME), any());
    verify(locker, times(PROCESS_CNT)).unlockProcess(eq(PIPELINE_NAME), any());
    assertThat(pool.activeProcessCount()).isZero();
    assertThat(pool.get().size()).isZero();
  }

  @Test
  public void testException() {
    PipeliteLocker locker = mock(PipeliteLocker.class);
    when(locker.lockProcess(any(), any())).thenReturn(true);

    ProcessLauncherPool pool =
        new ProcessLauncherPool(
            () -> {
              ProcessLauncher processLauncher = mock(ProcessLauncher.class);
              doThrow(new RuntimeException()).when(processLauncher).run(any(), any());
              return processLauncher;
            });

    ProcessLauncherStats stats = new ProcessLauncherStats();

    for (int i = 0; i < PROCESS_CNT; i++) {
      Process process =
          new ProcessBuilder("PROCESS" + i)
              .execute("STAGE1")
              .withEmptySyncExecutor(StageExecutionResultType.SUCCESS)
              .build();
      ProcessEntity processEntity = new ProcessEntity();
      process.setProcessEntity(processEntity);
      pool.run(PIPELINE_NAME, process, locker, (p, r) -> stats.add(p, r));
    }

    pool.shutDown();

    assertThat(stats.getProcessExecutionCount(ProcessState.COMPLETED)).isZero();
    assertThat(stats.getProcessExecutionCount(ProcessState.FAILED)).isZero();
    assertThat(stats.getProcessExceptionCount()).isEqualTo(PROCESS_CNT);
    verify(locker, times(PROCESS_CNT)).lockProcess(eq(PIPELINE_NAME), any());
    verify(locker, times(PROCESS_CNT)).unlockProcess(eq(PIPELINE_NAME), any());
    assertThat(pool.activeProcessCount()).isZero();
    assertThat(pool.get().size()).isZero();
  }
}
