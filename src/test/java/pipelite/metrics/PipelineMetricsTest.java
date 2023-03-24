/*
 * Copyright 2020-2022 EMBL - European Bioinformatics Institute
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
import pipelite.metrics.collector.ProcessRunnerMetrics;
import pipelite.process.ProcessState;
import pipelite.stage.executor.StageExecutorResult;

public class PipelineMetricsTest {

  @Test
  public void processCompleted() {
    ProcessRunnerMetrics metrics =
        new ProcessRunnerMetrics("PIPELINE_NAME", new SimpleMeterRegistry());

    String stageName = "STAGE_NAME";

    assertThat(metrics.completedCount()).isZero();
    assertThat(metrics.failedCount()).isZero();
    assertThat(metrics.stage(stageName).successCount()).isZero();
    assertThat(metrics.stage(stageName).failedCount()).isZero();

    metrics.stage(stageName).endStageExecution(StageExecutorResult.success());
    metrics.stage(stageName).endStageExecution(StageExecutorResult.executionError());
    metrics.endProcessExecution(ProcessState.COMPLETED);

    assertThat(metrics.completedCount()).isEqualTo(1);
    assertThat(metrics.failedCount()).isZero();
    assertThat(metrics.stage(stageName).successCount()).isEqualTo(1);
    assertThat(metrics.stage(stageName).failedCount()).isEqualTo(1);
  }

  @Test
  public void processFailed() {
    ProcessRunnerMetrics metrics =
        new ProcessRunnerMetrics("PIPELINE_NAME", new SimpleMeterRegistry());

    String stageName = "STAGE_NAME";

    assertThat(metrics.completedCount()).isZero();
    assertThat(metrics.failedCount()).isZero();
    assertThat(metrics.stage(stageName).successCount()).isZero();
    assertThat(metrics.stage(stageName).failedCount()).isZero();

    metrics.stage(stageName).endStageExecution(StageExecutorResult.success());
    metrics.stage(stageName).endStageExecution(StageExecutorResult.executionError());
    metrics.endProcessExecution(ProcessState.FAILED);

    assertThat(metrics.completedCount()).isZero();
    assertThat(metrics.failedCount()).isEqualTo(1);
    assertThat(metrics.stage(stageName).successCount()).isEqualTo(1);
    assertThat(metrics.stage(stageName).failedCount()).isEqualTo(1);
  }
}
