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

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import pipelite.process.ProcessState;

import static org.assertj.core.api.Assertions.*;

public class ProcessRunnerMetricsTest {

  @Test
  public void internalError() {
    ProcessRunnerMetrics metrics =
        new ProcessRunnerMetrics("PIPELINE_NAME", new SimpleMeterRegistry());

    Duration since = Duration.ofDays(1);

    assertThat(metrics.getInternalErrorCount()).isZero();
    assertThat(metrics.getInternalErrorCount(since)).isZero();

    metrics.internalError();

    assertThat(metrics.getInternalErrorCount()).isOne();
    assertThat(metrics.getInternalErrorCount(since)).isOne();

    Duration now = Duration.ofMinutes(0);

    assertThat(metrics.getInternalErrorCount()).isOne();
    assertThat(metrics.getInternalErrorCount(now)).isZero();

    metrics.purgeCustomCounters(now);

    assertThat(metrics.getInternalErrorCount()).isOne();
    assertThat(metrics.getInternalErrorCount(since)).isZero();
  }

  @Test
  public void processRunnerResultCompleted() {
    ProcessRunnerMetrics metrics =
        new ProcessRunnerMetrics("PIPELINE_NAME", new SimpleMeterRegistry());

    assertThat(metrics.getProcessCompletedCount()).isZero();
    assertThat(metrics.getProcessFailedCount()).isZero();
    assertThat(metrics.getStageSuccessCount()).isZero();
    assertThat(metrics.getStageFailedCount()).isZero();

    Duration since = Duration.ofDays(1);

    assertThat(metrics.getProcessCompletedCount(since)).isZero();
    assertThat(metrics.getProcessFailedCount(since)).isZero();
    assertThat(metrics.getStageSuccessCount(since)).isZero();
    assertThat(metrics.getStageFailedCount(since)).isZero();

    ProcessRunnerResult result = new ProcessRunnerResult();
    result.stageSuccess();
    result.stageFailed();
    result.internalError();
    metrics.processRunnerResult(ProcessState.COMPLETED, result);

    assertThat(metrics.getProcessCompletedCount()).isEqualTo(1);
    assertThat(metrics.getProcessFailedCount()).isZero();
    assertThat(metrics.getStageSuccessCount()).isEqualTo(1);
    assertThat(metrics.getStageFailedCount()).isEqualTo(1);

    assertThat(metrics.getProcessCompletedCount(since)).isEqualTo(1);
    assertThat(metrics.getProcessFailedCount(since)).isZero();
    assertThat(metrics.getStageSuccessCount(since)).isEqualTo(1);
    assertThat(metrics.getStageFailedCount(since)).isEqualTo(1);

    Duration now = Duration.ofMinutes(0);

    assertThat(metrics.getProcessCompletedCount(now)).isZero();
    assertThat(metrics.getProcessFailedCount(now)).isZero();
    assertThat(metrics.getStageSuccessCount(now)).isZero();
    assertThat(metrics.getStageFailedCount(now)).isZero();

    metrics.purgeCustomCounters(now);

    assertThat(metrics.getProcessCompletedCount()).isEqualTo(1);
    assertThat(metrics.getProcessFailedCount()).isZero();
    assertThat(metrics.getStageSuccessCount()).isEqualTo(1);
    assertThat(metrics.getStageFailedCount()).isEqualTo(1);

    assertThat(metrics.getProcessCompletedCount(since)).isZero();
    assertThat(metrics.getProcessFailedCount(since)).isZero();
    assertThat(metrics.getStageSuccessCount(since)).isZero();
    assertThat(metrics.getStageFailedCount(since)).isZero();
  }

  @Test
  public void processRunnerResultFailed() {
    ProcessRunnerMetrics metrics =
        new ProcessRunnerMetrics("PIPELINE_NAME", new SimpleMeterRegistry());

    assertThat(metrics.getProcessCompletedCount()).isZero();
    assertThat(metrics.getProcessFailedCount()).isZero();
    assertThat(metrics.getStageSuccessCount()).isZero();
    assertThat(metrics.getStageFailedCount()).isZero();

    Duration since = Duration.ofDays(1);

    assertThat(metrics.getProcessCompletedCount(since)).isZero();
    assertThat(metrics.getProcessFailedCount(since)).isZero();
    assertThat(metrics.getStageSuccessCount(since)).isZero();
    assertThat(metrics.getStageFailedCount(since)).isZero();

    ProcessRunnerResult result = new ProcessRunnerResult();
    result.stageSuccess();
    result.stageFailed();
    result.internalError();
    metrics.processRunnerResult(ProcessState.FAILED, result);

    assertThat(metrics.getProcessCompletedCount()).isZero();
    assertThat(metrics.getProcessFailedCount()).isEqualTo(1);
    assertThat(metrics.getStageSuccessCount()).isEqualTo(1);
    assertThat(metrics.getStageFailedCount()).isEqualTo(1);

    assertThat(metrics.getProcessCompletedCount(since)).isZero();
    assertThat(metrics.getProcessFailedCount(since)).isEqualTo(1);
    assertThat(metrics.getStageSuccessCount(since)).isEqualTo(1);
    assertThat(metrics.getStageFailedCount(since)).isEqualTo(1);

    Duration now = Duration.ofMinutes(0);

    assertThat(metrics.getProcessCompletedCount(now)).isZero();
    assertThat(metrics.getProcessFailedCount(now)).isZero();
    assertThat(metrics.getStageSuccessCount(now)).isZero();
    assertThat(metrics.getStageFailedCount(now)).isZero();

    metrics.purgeCustomCounters(now);

    assertThat(metrics.getProcessCompletedCount()).isZero();
    assertThat(metrics.getProcessFailedCount()).isEqualTo(1);
    assertThat(metrics.getStageSuccessCount()).isEqualTo(1);
    assertThat(metrics.getStageFailedCount()).isEqualTo(1);

    assertThat(metrics.getProcessCompletedCount(since)).isZero();
    assertThat(metrics.getProcessFailedCount(since)).isZero();
    assertThat(metrics.getStageSuccessCount(since)).isZero();
    assertThat(metrics.getStageFailedCount(since)).isZero();
  }
}
