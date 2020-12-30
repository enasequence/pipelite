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
package pipelite.metrics;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.util.List;

import org.junit.jupiter.api.Test;
import pipelite.launcher.process.runner.ProcessRunnerResult;
import pipelite.process.ProcessState;
import tech.tablesaw.api.Table;
import tech.tablesaw.plotly.Plot;
import tech.tablesaw.plotly.components.Figure;
import tech.tablesaw.plotly.components.Layout;
import tech.tablesaw.plotly.traces.ScatterTrace;
import tech.tablesaw.table.TableSliceGroup;

import static org.assertj.core.api.Assertions.*;

public class PipelineMetricsTest {

  @Test
  public void internalError() {
    PipelineMetrics metrics = new PipelineMetrics("PIPELINE_NAME", new SimpleMeterRegistry());

    assertThat(metrics.getInternalErrorCount()).isZero();
    assertThat(metrics.getInternalErrorTimeSeries().rowCount()).isZero();
    assertThat(metrics.getInternalErrorTimeSeries().dateTimeColumn("time")).isNotNull();
    assertThat(metrics.getInternalErrorTimeSeries().doubleColumn("count")).isNotNull();

    metrics.incrementInternalErrorCount();

    assertThat(metrics.getInternalErrorCount()).isOne();
    assertThat(metrics.getInternalErrorTimeSeries().rowCount()).isOne();
  }

  @Test
  public void processCompleted() {
    PipelineMetrics metrics = new PipelineMetrics("PIPELINE_NAME", new SimpleMeterRegistry());

    assertThat(metrics.process().getCompletedCount()).isZero();
    assertThat(metrics.process().getFailedCount()).isZero();
    assertThat(metrics.stage().getSuccessCount()).isZero();
    assertThat(metrics.stage().getFailedCount()).isZero();
    assertThat(TimeSeriesMetrics.getCount(metrics.process().getCompletedTimeSeries())).isZero();
    assertThat(TimeSeriesMetrics.getCount(metrics.process().getFailedTimeSeries())).isZero();
    assertThat(TimeSeriesMetrics.getCount(metrics.stage().getSuccessTimeSeries())).isZero();
    assertThat(TimeSeriesMetrics.getCount(metrics.stage().getFailedTimeSeries())).isZero();

    ProcessRunnerResult result = new ProcessRunnerResult();
    result.stageSuccess();
    result.stageFailed();
    result.internalError();
    metrics.increment(ProcessState.COMPLETED, result);

    assertThat(metrics.process().getCompletedCount()).isEqualTo(1);
    assertThat(metrics.process().getFailedCount()).isZero();
    assertThat(metrics.stage().getSuccessCount()).isEqualTo(1);
    assertThat(metrics.stage().getFailedCount()).isEqualTo(1);
    assertThat(TimeSeriesMetrics.getCount(metrics.process().getCompletedTimeSeries())).isEqualTo(1);
    assertThat(TimeSeriesMetrics.getCount(metrics.process().getFailedTimeSeries())).isZero();
    assertThat(TimeSeriesMetrics.getCount(metrics.stage().getSuccessTimeSeries())).isEqualTo(1);
    assertThat(TimeSeriesMetrics.getCount(metrics.stage().getFailedTimeSeries())).isEqualTo(1);
  }

  @Test
  public void processFailed() {
    PipelineMetrics metrics = new PipelineMetrics("PIPELINE_NAME", new SimpleMeterRegistry());

    assertThat(metrics.process().getCompletedCount()).isZero();
    assertThat(metrics.process().getFailedCount()).isZero();
    assertThat(metrics.stage().getSuccessCount()).isZero();
    assertThat(metrics.stage().getFailedCount()).isZero();
    assertThat(TimeSeriesMetrics.getCount(metrics.process().getCompletedTimeSeries())).isZero();
    assertThat(TimeSeriesMetrics.getCount(metrics.process().getFailedTimeSeries())).isZero();
    assertThat(TimeSeriesMetrics.getCount(metrics.stage().getSuccessTimeSeries())).isZero();
    assertThat(TimeSeriesMetrics.getCount(metrics.stage().getFailedTimeSeries())).isZero();

    ProcessRunnerResult result = new ProcessRunnerResult();
    result.stageSuccess();
    result.stageFailed();
    result.internalError();
    metrics.increment(ProcessState.FAILED, result);

    assertThat(metrics.process().getCompletedCount()).isZero();
    assertThat(metrics.process().getFailedCount()).isEqualTo(1);
    assertThat(metrics.stage().getSuccessCount()).isEqualTo(1);
    assertThat(metrics.stage().getFailedCount()).isEqualTo(1);

    assertThat(TimeSeriesMetrics.getCount(metrics.process().getCompletedTimeSeries())).isZero();
    assertThat(TimeSeriesMetrics.getCount(metrics.process().getFailedTimeSeries())).isEqualTo(1);
    assertThat(TimeSeriesMetrics.getCount(metrics.stage().getSuccessTimeSeries())).isEqualTo(1);
    assertThat(TimeSeriesMetrics.getCount(metrics.stage().getFailedTimeSeries())).isEqualTo(1);
  }
}
