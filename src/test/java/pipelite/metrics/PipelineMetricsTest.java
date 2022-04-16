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

import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import pipelite.metrics.helper.TimeSeriesHelper;
import pipelite.process.ProcessState;
import pipelite.stage.executor.StageExecutorResult;

public class PipelineMetricsTest {

  @Test
  public void processCompleted() {
    ProcessMetrics metrics = new ProcessMetrics("PIPELINE_NAME", new SimpleMeterRegistry());

    String stageName = "STAGE_NAME";

    assertThat(metrics.runner().completedCount()).isZero();
    assertThat(metrics.runner().failedCount()).isZero();
    assertThat(metrics.stage(stageName).runner().successCount()).isZero();
    assertThat(metrics.stage(stageName).runner().failedCount()).isZero();
    assertThat(TimeSeriesHelper.getCount(metrics.runner().completedTimeSeries())).isZero();
    assertThat(TimeSeriesHelper.getCount(metrics.runner().failedTimeSeries())).isZero();

    metrics.stage(stageName).runner().endStageExecution(StageExecutorResult.success());
    metrics.stage(stageName).runner().endStageExecution(StageExecutorResult.error());
    metrics.runner().endProcessExecution(ProcessState.COMPLETED);

    assertThat(metrics.runner().completedCount()).isEqualTo(1);
    assertThat(metrics.runner().failedCount()).isZero();
    assertThat(metrics.stage(stageName).runner().successCount()).isEqualTo(1);
    assertThat(metrics.stage(stageName).runner().failedCount()).isEqualTo(1);
    assertThat(TimeSeriesHelper.getCount(metrics.runner().completedTimeSeries())).isEqualTo(1);
    assertThat(TimeSeriesHelper.getCount(metrics.runner().failedTimeSeries())).isZero();
  }

  @Test
  public void processFailed() {
    ProcessMetrics metrics = new ProcessMetrics("PIPELINE_NAME", new SimpleMeterRegistry());

    String stageName = "STAGE_NAME";

    assertThat(metrics.runner().completedCount()).isZero();
    assertThat(metrics.runner().failedCount()).isZero();
    assertThat(metrics.stage(stageName).runner().successCount()).isZero();
    assertThat(metrics.stage(stageName).runner().failedCount()).isZero();
    assertThat(TimeSeriesHelper.getCount(metrics.runner().completedTimeSeries())).isZero();
    assertThat(TimeSeriesHelper.getCount(metrics.runner().failedTimeSeries())).isZero();

    metrics.stage(stageName).runner().endStageExecution(StageExecutorResult.success());
    metrics.stage(stageName).runner().endStageExecution(StageExecutorResult.error());
    metrics.runner().endProcessExecution(ProcessState.FAILED);

    assertThat(metrics.runner().completedCount()).isZero();
    assertThat(metrics.runner().failedCount()).isEqualTo(1);
    assertThat(metrics.stage(stageName).runner().successCount()).isEqualTo(1);
    assertThat(metrics.stage(stageName).runner().failedCount()).isEqualTo(1);

    assertThat(TimeSeriesHelper.getCount(metrics.runner().completedTimeSeries())).isZero();
    assertThat(TimeSeriesHelper.getCount(metrics.runner().failedTimeSeries())).isEqualTo(1);
  }
}
