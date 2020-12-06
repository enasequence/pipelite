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

public class DefaultProcessRunnerPoolTest {

  private static final int PROCESS_CNT = 1000;
  public static final String PIPELINE_NAME = "PIPELINE1";

  private Supplier<ProcessRunner> processRunnerSupplier(ProcessState state) {
    return () -> {
      ProcessRunner processRunner = mock(ProcessRunner.class);
      doAnswer(
              i -> {
                Process process = i.getArgument(1);
                ProcessRunnerCallback callback = i.getArgument(2);
                process.getProcessEntity().endExecution(state);
                ProcessRunnerResult result = new ProcessRunnerResult();
                result.setProcessExecutionCount(1);
                callback.accept(process, result);
                return null;
              })
          .when(processRunner)
          .runProcess(any(), any(), any());
      return processRunner;
    };
  }

  @Test
  public void testSuccess() {
    PipeliteLocker locker = mock(PipeliteLocker.class);
    when(locker.lockProcess(any(), any())).thenReturn(true);

    DefaultProcessRunnerPool pool =
        new DefaultProcessRunnerPool(locker, processRunnerSupplier(ProcessState.COMPLETED));

    ProcessLauncherStats stats = new ProcessLauncherStats();

    for (int i = 0; i < PROCESS_CNT; i++) {
      Process process =
          new ProcessBuilder("PROCESS" + i)
              .execute("STAGE1")
              .withEmptySyncExecutor(StageExecutionResultType.SUCCESS)
              .build();
      ProcessEntity processEntity = new ProcessEntity();
      process.setProcessEntity(processEntity);
      pool.runProcess(PIPELINE_NAME, process, (p, r) -> stats.add(p, r));
    }

    pool.shutDown();

    assertThat(stats.getProcessExecutionCount(ProcessState.COMPLETED)).isEqualTo(PROCESS_CNT);
    assertThat(stats.getProcessExecutionCount(ProcessState.FAILED)).isZero();
    assertThat(stats.getProcessExceptionCount()).isZero();
    verify(locker, times(PROCESS_CNT)).lockProcess(eq(PIPELINE_NAME), any());
    verify(locker, times(PROCESS_CNT)).unlockProcess(eq(PIPELINE_NAME), any());
    assertThat(pool.getActiveProcessRunnerCount()).isZero();
    assertThat(pool.getActiveProcessRunners().size()).isZero();
  }

  @Test
  public void testFailed() {
    PipeliteLocker locker = mock(PipeliteLocker.class);
    when(locker.lockProcess(any(), any())).thenReturn(true);

    DefaultProcessRunnerPool pool =
        new DefaultProcessRunnerPool(locker, processRunnerSupplier(ProcessState.FAILED));

    ProcessLauncherStats stats = new ProcessLauncherStats();

    for (int i = 0; i < PROCESS_CNT; i++) {
      Process process =
          new ProcessBuilder("PROCESS" + i)
              .execute("STAGE1")
              .withEmptySyncExecutor(StageExecutionResultType.SUCCESS)
              .build();
      ProcessEntity processEntity = new ProcessEntity();
      process.setProcessEntity(processEntity);
      pool.runProcess(PIPELINE_NAME, process, (p, r) -> stats.add(p, r));
    }

    pool.shutDown();

    assertThat(stats.getProcessExecutionCount(ProcessState.COMPLETED)).isZero();
    assertThat(stats.getProcessExecutionCount(ProcessState.FAILED)).isEqualTo(PROCESS_CNT);
    assertThat(stats.getProcessExceptionCount()).isZero();
    verify(locker, times(PROCESS_CNT)).lockProcess(eq(PIPELINE_NAME), any());
    verify(locker, times(PROCESS_CNT)).unlockProcess(eq(PIPELINE_NAME), any());
    assertThat(pool.getActiveProcessRunnerCount()).isZero();
    assertThat(pool.getActiveProcessRunners().size()).isZero();
  }

  @Test
  public void testException() {
    PipeliteLocker locker = mock(PipeliteLocker.class);
    when(locker.lockProcess(any(), any())).thenReturn(true);
    DefaultProcessRunnerPool pool =
        new DefaultProcessRunnerPool(
            locker,
            () -> {
              ProcessRunner processRunner = mock(ProcessRunner.class);
              doThrow(new RuntimeException()).when(processRunner).runProcess(any(), any(), any());
              return processRunner;
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
      pool.runProcess(PIPELINE_NAME, process, (p, r) -> stats.add(p, r));
    }

    pool.shutDown();

    assertThat(stats.getProcessExecutionCount(ProcessState.COMPLETED)).isZero();
    assertThat(stats.getProcessExecutionCount(ProcessState.FAILED)).isZero();
    assertThat(stats.getProcessExceptionCount()).isEqualTo(PROCESS_CNT);
    verify(locker, times(PROCESS_CNT)).lockProcess(eq(PIPELINE_NAME), any());
    verify(locker, times(PROCESS_CNT)).unlockProcess(eq(PIPELINE_NAME), any());
    assertThat(pool.getActiveProcessRunnerCount()).isZero();
    assertThat(pool.getActiveProcessRunners().size()).isZero();
  }
}
