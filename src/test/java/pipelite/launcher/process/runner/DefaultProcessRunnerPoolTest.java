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

import org.junit.jupiter.api.Test;
import pipelite.PipeliteMetricsTestFactory;
import pipelite.configuration.ServiceConfiguration;
import pipelite.entity.ProcessEntity;
import pipelite.lock.PipeliteLocker;
import pipelite.metrics.PipelineMetrics;
import pipelite.metrics.PipeliteMetrics;
import pipelite.metrics.TimeSeriesMetrics;
import pipelite.process.Process;
import pipelite.process.ProcessState;
import pipelite.process.builder.ProcessBuilder;
import pipelite.repository.InternalErrorRepository;
import pipelite.service.InternalErrorService;

import java.time.ZonedDateTime;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;
import static org.mockito.internal.verification.VerificationModeFactory.times;

public class DefaultProcessRunnerPoolTest {

  private static final int PROCESS_CNT = 1000;
  public static final String PIPELINE_NAME = "PIPELINE1";

  private Function<String, ProcessRunner> processRunnerSupplier(ProcessState state) {
    return (pipelineName) -> {
      ProcessRunner processRunner = mock(ProcessRunner.class);
      doAnswer(
              i -> {
                Process process = i.getArgument(0);
                process.getProcessEntity().endExecution(state);
                return new ProcessRunnerResult();
              })
          .when(processRunner)
          .runProcess(any());
      return processRunner;
    };
  }

  @Test
  public void testSuccess() {
    ServiceConfiguration serviceConfiguration = mock(ServiceConfiguration.class);
    InternalErrorService internalErrorService = mock(InternalErrorService.class);
    PipeliteLocker locker = mock(PipeliteLocker.class);
    when(locker.lockProcess(any(), any())).thenReturn(true);

    PipeliteMetrics metrics = PipeliteMetricsTestFactory.pipeliteMetrics();

    DefaultProcessRunnerPool pool =
        new DefaultProcessRunnerPool(
            serviceConfiguration,
            internalErrorService,
            locker,
            processRunnerSupplier(ProcessState.COMPLETED),
            metrics);

    AtomicInteger runProcessCount = new AtomicInteger();

    for (int i = 0; i < PROCESS_CNT; i++) {
      Process process =
          new ProcessBuilder("PROCESS" + i).execute("STAGE1").withCallExecutor().build();
      ProcessEntity processEntity = new ProcessEntity();
      process.setProcessEntity(processEntity);
      pool.runProcess(PIPELINE_NAME, process, (p, r) -> runProcessCount.incrementAndGet());
    }

    pool.shutDown();

    PipelineMetrics pipelineMetrics = metrics.pipeline(PIPELINE_NAME);

    assertThat(runProcessCount.get()).isEqualTo(PROCESS_CNT);

    assertThat(pipelineMetrics.process().getCompletedCount()).isEqualTo(PROCESS_CNT);
    assertThat(pipelineMetrics.process().getFailedCount()).isZero();
    assertThat(pipelineMetrics.getInternalErrorCount()).isZero();
    assertThat(TimeSeriesMetrics.getCount(pipelineMetrics.process().getCompletedTimeSeries()))
        .isEqualTo(PROCESS_CNT);
    assertThat(
            TimeSeriesMetrics.getCount(
                pipelineMetrics.process().getCompletedTimeSeries(),
                ZonedDateTime.now().minusHours(1)))
        .isEqualTo(PROCESS_CNT);
    assertThat(
            TimeSeriesMetrics.getCount(
                pipelineMetrics.process().getCompletedTimeSeries(),
                ZonedDateTime.now().plusHours(1)))
        .isZero();
    assertThat(TimeSeriesMetrics.getCount(pipelineMetrics.process().getFailedTimeSeries()))
        .isZero();
    assertThat(TimeSeriesMetrics.getCount(pipelineMetrics.getInternalErrorTimeSeries())).isZero();

    verify(locker, times(PROCESS_CNT)).lockProcess(eq(PIPELINE_NAME), any());
    verify(locker, times(PROCESS_CNT)).unlockProcess(eq(PIPELINE_NAME), any());
    assertThat(pool.getActiveProcessCount()).isZero();
    assertThat(pool.getActiveProcessRunners().size()).isZero();
  }

  @Test
  public void testFailed() {
    ServiceConfiguration serviceConfiguration = mock(ServiceConfiguration.class);
    InternalErrorService internalErrorService = mock(InternalErrorService.class);
    PipeliteLocker locker = mock(PipeliteLocker.class);
    when(locker.lockProcess(any(), any())).thenReturn(true);

    PipeliteMetrics metrics = PipeliteMetricsTestFactory.pipeliteMetrics();

    DefaultProcessRunnerPool pool =
        new DefaultProcessRunnerPool(
            serviceConfiguration,
            internalErrorService,
            locker,
            processRunnerSupplier(ProcessState.FAILED),
            metrics);

    AtomicInteger runProcessCount = new AtomicInteger();

    for (int i = 0; i < PROCESS_CNT; i++) {
      Process process =
          new ProcessBuilder("PROCESS" + i).execute("STAGE1").withCallExecutor().build();
      ProcessEntity processEntity = new ProcessEntity();
      process.setProcessEntity(processEntity);
      pool.runProcess(PIPELINE_NAME, process, (p, r) -> runProcessCount.incrementAndGet());
    }

    pool.shutDown();

    PipelineMetrics pipelineMetrics = metrics.pipeline(PIPELINE_NAME);

    assertThat(runProcessCount.get()).isEqualTo(PROCESS_CNT);

    assertThat(pipelineMetrics.process().getCompletedCount()).isZero();
    assertThat(pipelineMetrics.process().getFailedCount()).isEqualTo(PROCESS_CNT);
    assertThat(pipelineMetrics.getInternalErrorCount()).isZero();
    assertThat(TimeSeriesMetrics.getCount(pipelineMetrics.process().getCompletedTimeSeries()))
        .isZero();
    assertThat(TimeSeriesMetrics.getCount(pipelineMetrics.process().getFailedTimeSeries()))
        .isEqualTo(PROCESS_CNT);
    assertThat(
            TimeSeriesMetrics.getCount(
                pipelineMetrics.process().getFailedTimeSeries(), ZonedDateTime.now().minusHours(1)))
        .isEqualTo(PROCESS_CNT);
    assertThat(
            TimeSeriesMetrics.getCount(
                pipelineMetrics.process().getFailedTimeSeries(), ZonedDateTime.now().plusHours(1)))
        .isZero();
    assertThat(TimeSeriesMetrics.getCount(pipelineMetrics.getInternalErrorTimeSeries())).isZero();

    verify(locker, times(PROCESS_CNT)).lockProcess(eq(PIPELINE_NAME), any());
    verify(locker, times(PROCESS_CNT)).unlockProcess(eq(PIPELINE_NAME), any());
    assertThat(pool.getActiveProcessCount()).isZero();
    assertThat(pool.getActiveProcessRunners().size()).isZero();
  }

  @Test
  public void testException() {
    ServiceConfiguration serviceConfiguration = mock(ServiceConfiguration.class);
    PipeliteMetrics metrics = PipeliteMetricsTestFactory.pipeliteMetrics();
    InternalErrorService internalErrorService =
        new InternalErrorService(mock(InternalErrorRepository.class), metrics);
    PipeliteLocker locker = mock(PipeliteLocker.class);
    when(locker.lockProcess(any(), any())).thenReturn(true);

    DefaultProcessRunnerPool pool =
        new DefaultProcessRunnerPool(
            serviceConfiguration,
            internalErrorService,
            locker,
            (pipelineName) -> {
              ProcessRunner processRunner = mock(ProcessRunner.class);
              doThrow(new RuntimeException()).when(processRunner).runProcess(any());
              return processRunner;
            },
            metrics);

    AtomicInteger runProcessCount = new AtomicInteger();

    for (int i = 0; i < PROCESS_CNT; i++) {
      Process process =
          new ProcessBuilder("PROCESS" + i).execute("STAGE1").withCallExecutor().build();
      ProcessEntity processEntity = new ProcessEntity();
      process.setProcessEntity(processEntity);
      pool.runProcess(PIPELINE_NAME, process, (p, r) -> runProcessCount.incrementAndGet());
    }

    pool.shutDown();

    PipelineMetrics pipelineMetrics = metrics.pipeline(PIPELINE_NAME);

    assertThat(runProcessCount.get()).isEqualTo(PROCESS_CNT);

    assertThat(pipelineMetrics.process().getCompletedCount()).isZero();
    assertThat(pipelineMetrics.process().getFailedCount()).isZero();
    assertThat(pipelineMetrics.getInternalErrorCount()).isEqualTo(PROCESS_CNT);
    assertThat(TimeSeriesMetrics.getCount(pipelineMetrics.process().getCompletedTimeSeries()))
        .isZero();
    assertThat(TimeSeriesMetrics.getCount(pipelineMetrics.process().getFailedTimeSeries()))
        .isZero();
    assertThat(TimeSeriesMetrics.getCount(pipelineMetrics.getInternalErrorTimeSeries()))
        .isEqualTo(PROCESS_CNT);
    assertThat(
            TimeSeriesMetrics.getCount(
                pipelineMetrics.getInternalErrorTimeSeries(), ZonedDateTime.now().minusHours(1)))
        .isEqualTo(PROCESS_CNT);
    assertThat(
            TimeSeriesMetrics.getCount(
                pipelineMetrics.getInternalErrorTimeSeries(), ZonedDateTime.now().plusHours(1)))
        .isZero();

    verify(locker, times(PROCESS_CNT)).lockProcess(eq(PIPELINE_NAME), any());
    verify(locker, times(PROCESS_CNT)).unlockProcess(eq(PIPELINE_NAME), any());
    assertThat(pool.getActiveProcessCount()).isZero();
    assertThat(pool.getActiveProcessRunners().size()).isZero();
  }
}
