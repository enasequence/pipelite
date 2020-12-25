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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.mockito.internal.verification.VerificationModeFactory.times;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.util.function.Supplier;
import org.junit.jupiter.api.Test;
import pipelite.entity.ProcessEntity;
import pipelite.lock.PipeliteLocker;
import pipelite.process.Process;
import pipelite.process.ProcessState;
import pipelite.process.builder.ProcessBuilder;
import pipelite.stage.executor.StageExecutorResultType;

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

    ProcessRunnerStats stats = new ProcessRunnerStats(PIPELINE_NAME, new SimpleMeterRegistry());

    for (int i = 0; i < PROCESS_CNT; i++) {
      Process process =
          new ProcessBuilder("PROCESS" + i)
              .execute("STAGE1")
              .withEmptySyncExecutor(StageExecutorResultType.SUCCESS)
              .build();
      ProcessEntity processEntity = new ProcessEntity();
      process.setProcessEntity(processEntity);
      pool.runProcess(
          PIPELINE_NAME,
          process,
          (p, r) -> stats.addProcessRunnerResult(p.getProcessEntity().getState(), r));
    }

    pool.shutDown();

    assertThat(stats.getCompletedProcessCount()).isEqualTo(PROCESS_CNT);
    assertThat(stats.getFailedProcessCount()).isZero();
    assertThat(stats.getProcessExceptionCount()).isZero();

    Duration before = Duration.ofMinutes(1);

    assertThat(stats.getProcessCompletedCount(before)).isEqualTo(PROCESS_CNT);
    assertThat(stats.getProcessFailedCount(before)).isZero();
    assertThat(stats.getProcessExceptionCount(before)).isZero();
    assertThat(stats.getProcessCompletedCount(Duration.ofNanos(0))).isZero();
    assertThat(stats.getProcessFailedCount(Duration.ofNanos(0))).isZero();
    assertThat(stats.getProcessExceptionCount(Duration.ofNanos(0))).isZero();

    verify(locker, times(PROCESS_CNT)).lockProcess(eq(PIPELINE_NAME), any());
    verify(locker, times(PROCESS_CNT)).unlockProcess(eq(PIPELINE_NAME), any());
    assertThat(pool.getActiveProcessCount()).isZero();
    assertThat(pool.getActiveProcessRunners().size()).isZero();
  }

  @Test
  public void testFailed() {
    PipeliteLocker locker = mock(PipeliteLocker.class);
    when(locker.lockProcess(any(), any())).thenReturn(true);

    DefaultProcessRunnerPool pool =
        new DefaultProcessRunnerPool(locker, processRunnerSupplier(ProcessState.FAILED));

    ProcessRunnerStats stats = new ProcessRunnerStats(PIPELINE_NAME, new SimpleMeterRegistry());

    for (int i = 0; i < PROCESS_CNT; i++) {
      Process process =
          new ProcessBuilder("PROCESS" + i)
              .execute("STAGE1")
              .withEmptySyncExecutor(StageExecutorResultType.SUCCESS)
              .build();
      ProcessEntity processEntity = new ProcessEntity();
      process.setProcessEntity(processEntity);
      pool.runProcess(
          PIPELINE_NAME,
          process,
          (p, r) -> stats.addProcessRunnerResult(p.getProcessEntity().getState(), r));
    }

    pool.shutDown();

    assertThat(stats.getCompletedProcessCount()).isZero();
    assertThat(stats.getFailedProcessCount()).isEqualTo(PROCESS_CNT);
    assertThat(stats.getProcessExceptionCount()).isZero();

    Duration before = Duration.ofMinutes(1);

    assertThat(stats.getProcessCompletedCount(before)).isZero();
    assertThat(stats.getProcessFailedCount(before)).isEqualTo(PROCESS_CNT);
    assertThat(stats.getProcessExceptionCount(before)).isZero();
    assertThat(stats.getProcessCompletedCount(Duration.ofNanos(0))).isZero();
    assertThat(stats.getProcessFailedCount(Duration.ofNanos(0))).isZero();
    assertThat(stats.getProcessExceptionCount(Duration.ofNanos(0))).isZero();

    verify(locker, times(PROCESS_CNT)).lockProcess(eq(PIPELINE_NAME), any());
    verify(locker, times(PROCESS_CNT)).unlockProcess(eq(PIPELINE_NAME), any());
    assertThat(pool.getActiveProcessCount()).isZero();
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

    ProcessRunnerStats stats = new ProcessRunnerStats(PIPELINE_NAME, new SimpleMeterRegistry());

    for (int i = 0; i < PROCESS_CNT; i++) {
      Process process =
          new ProcessBuilder("PROCESS" + i)
              .execute("STAGE1")
              .withEmptySyncExecutor(StageExecutorResultType.SUCCESS)
              .build();
      ProcessEntity processEntity = new ProcessEntity();
      process.setProcessEntity(processEntity);
      pool.runProcess(
          PIPELINE_NAME,
          process,
          (p, r) -> stats.addProcessRunnerResult(p.getProcessEntity().getState(), r));
    }

    pool.shutDown();

    assertThat(stats.getCompletedProcessCount()).isZero();
    assertThat(stats.getFailedProcessCount()).isZero();
    assertThat(stats.getProcessExceptionCount()).isEqualTo(PROCESS_CNT);

    Duration before = Duration.ofMinutes(1);

    assertThat(stats.getProcessCompletedCount(before)).isZero();
    assertThat(stats.getProcessFailedCount(before)).isZero();
    assertThat(stats.getProcessExceptionCount(before)).isEqualTo(PROCESS_CNT);
    assertThat(stats.getProcessCompletedCount(Duration.ofNanos(0))).isZero();
    assertThat(stats.getProcessFailedCount(Duration.ofNanos(0))).isZero();
    assertThat(stats.getProcessExceptionCount(Duration.ofNanos(0))).isZero();

    verify(locker, times(PROCESS_CNT)).lockProcess(eq(PIPELINE_NAME), any());
    verify(locker, times(PROCESS_CNT)).unlockProcess(eq(PIPELINE_NAME), any());
    assertThat(pool.getActiveProcessCount()).isZero();
    assertThat(pool.getActiveProcessRunners().size()).isZero();
  }
}
