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

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import pipelite.process.ProcessState;

public class ProcessRunnerMetricsTest {

  @Test
  public void addProcessCreationFailed() {
    ProcessRunnerMetrics metrics =
        new ProcessRunnerMetrics("PIPELINE_NAME", new SimpleMeterRegistry());

    assertThat(metrics.getCompletedProcessCount()).isZero();
    assertThat(metrics.getFailedProcessCount()).isZero();
    assertThat(metrics.getProcessExceptionCount()).isZero();
    assertThat(metrics.getProcessCreationFailedCount()).isZero();
    assertThat(metrics.getSuccessfulStageCount()).isZero();
    assertThat(metrics.getFailedStageCount()).isZero();

    Duration since = Duration.ofDays(1);

    assertThat(metrics.getProcessCompletedCount(since)).isZero();
    assertThat(metrics.getProcessFailedCount(since)).isZero();
    assertThat(metrics.getProcessExceptionCount(since)).isZero();
    assertThat(metrics.getProcessCreationFailedCount(since)).isZero();
    assertThat(metrics.getStageSuccessCount(since)).isZero();
    assertThat(metrics.getStageFailedCount(since)).isZero();

    metrics.addProcessCreationFailed(10);

    assertThat(metrics.getCompletedProcessCount()).isZero();
    assertThat(metrics.getFailedProcessCount()).isZero();
    assertThat(metrics.getProcessExceptionCount()).isZero();
    assertThat(metrics.getProcessCreationFailedCount()).isEqualTo(10);
    assertThat(metrics.getSuccessfulStageCount()).isZero();
    assertThat(metrics.getFailedStageCount()).isZero();

    assertThat(metrics.getProcessCompletedCount(since)).isZero();
    assertThat(metrics.getProcessFailedCount(since)).isZero();
    assertThat(metrics.getProcessExceptionCount(since)).isZero();
    assertThat(metrics.getProcessCreationFailedCount(since)).isEqualTo(10);
    assertThat(metrics.getStageSuccessCount(since)).isZero();
    assertThat(metrics.getStageFailedCount(since)).isZero();

    Duration now = Duration.ofMinutes(0);

    assertThat(metrics.getProcessCompletedCount(now)).isZero();
    assertThat(metrics.getProcessFailedCount(now)).isZero();
    assertThat(metrics.getProcessExceptionCount(now)).isZero();
    assertThat(metrics.getProcessCreationFailedCount(now)).isZero();
    assertThat(metrics.getStageSuccessCount(now)).isZero();
    assertThat(metrics.getStageFailedCount(now)).isZero();

    metrics.purge(now);

    assertThat(metrics.getCompletedProcessCount()).isZero();
    assertThat(metrics.getFailedProcessCount()).isZero();
    assertThat(metrics.getProcessExceptionCount()).isZero();
    assertThat(metrics.getProcessCreationFailedCount()).isEqualTo(10);
    assertThat(metrics.getSuccessfulStageCount()).isZero();
    assertThat(metrics.getFailedStageCount()).isZero();

    assertThat(metrics.getProcessCompletedCount(since)).isZero();
    assertThat(metrics.getProcessFailedCount(since)).isZero();
    assertThat(metrics.getProcessExceptionCount(since)).isZero();
    assertThat(metrics.getProcessCreationFailedCount(since)).isZero();
    assertThat(metrics.getStageSuccessCount(since)).isZero();
    assertThat(metrics.getStageFailedCount(since)).isZero();
  }

  @Test
  public void addCompletedProcessRunnerResult() {
    ProcessRunnerMetrics metrics =
        new ProcessRunnerMetrics("PIPELINE_NAME", new SimpleMeterRegistry());

    assertThat(metrics.getCompletedProcessCount()).isZero();
    assertThat(metrics.getFailedProcessCount()).isZero();
    assertThat(metrics.getProcessExceptionCount()).isZero();
    assertThat(metrics.getProcessCreationFailedCount()).isZero();
    assertThat(metrics.getSuccessfulStageCount()).isZero();
    assertThat(metrics.getFailedStageCount()).isZero();

    Duration since = Duration.ofDays(1);

    assertThat(metrics.getProcessCompletedCount(since)).isZero();
    assertThat(metrics.getProcessFailedCount(since)).isZero();
    assertThat(metrics.getProcessExceptionCount(since)).isZero();
    assertThat(metrics.getProcessCreationFailedCount(since)).isZero();
    assertThat(metrics.getStageSuccessCount(since)).isZero();
    assertThat(metrics.getStageFailedCount(since)).isZero();

    ProcessRunnerResult result = new ProcessRunnerResult();
    result.addStageSuccessCount(5L);
    result.addStageFailedCount(10L);
    result.setProcessExceptionCount(15);
    result.setProcessExecutionCount(20);
    metrics.addProcessRunnerResult(ProcessState.COMPLETED, result);

    assertThat(metrics.getCompletedProcessCount()).isEqualTo(20);
    assertThat(metrics.getFailedProcessCount()).isZero();
    assertThat(metrics.getProcessExceptionCount()).isEqualTo(15);
    assertThat(metrics.getProcessCreationFailedCount()).isZero();
    assertThat(metrics.getSuccessfulStageCount()).isEqualTo(5);
    assertThat(metrics.getFailedStageCount()).isEqualTo(10);

    assertThat(metrics.getProcessCompletedCount(since)).isEqualTo(20);
    assertThat(metrics.getProcessFailedCount(since)).isZero();
    assertThat(metrics.getProcessExceptionCount(since)).isEqualTo(15);
    assertThat(metrics.getProcessCreationFailedCount(since)).isZero();
    assertThat(metrics.getStageSuccessCount(since)).isEqualTo(5);
    assertThat(metrics.getStageFailedCount(since)).isEqualTo(10);

    Duration now = Duration.ofMinutes(0);

    assertThat(metrics.getProcessCompletedCount(now)).isZero();
    assertThat(metrics.getProcessFailedCount(now)).isZero();
    assertThat(metrics.getProcessExceptionCount(now)).isZero();
    assertThat(metrics.getProcessCreationFailedCount(now)).isZero();
    assertThat(metrics.getStageSuccessCount(now)).isZero();
    assertThat(metrics.getStageFailedCount(now)).isZero();

    metrics.purge(now);

    assertThat(metrics.getCompletedProcessCount()).isEqualTo(20);
    assertThat(metrics.getFailedProcessCount()).isZero();
    assertThat(metrics.getProcessExceptionCount()).isEqualTo(15);
    assertThat(metrics.getProcessCreationFailedCount()).isZero();
    assertThat(metrics.getSuccessfulStageCount()).isEqualTo(5);
    assertThat(metrics.getFailedStageCount()).isEqualTo(10);

    assertThat(metrics.getProcessCompletedCount(since)).isZero();
    assertThat(metrics.getProcessFailedCount(since)).isZero();
    assertThat(metrics.getProcessExceptionCount(since)).isZero();
    assertThat(metrics.getProcessCreationFailedCount(since)).isZero();
    assertThat(metrics.getStageSuccessCount(since)).isZero();
    assertThat(metrics.getStageFailedCount(since)).isZero();
  }

  @Test
  public void addFailedProcessRunnerResult() {
    ProcessRunnerMetrics metrics =
        new ProcessRunnerMetrics("PIPELINE_NAME", new SimpleMeterRegistry());

    assertThat(metrics.getCompletedProcessCount()).isZero();
    assertThat(metrics.getFailedProcessCount()).isZero();
    assertThat(metrics.getProcessExceptionCount()).isZero();
    assertThat(metrics.getProcessCreationFailedCount()).isZero();
    assertThat(metrics.getSuccessfulStageCount()).isZero();
    assertThat(metrics.getFailedStageCount()).isZero();

    Duration since = Duration.ofDays(1);

    assertThat(metrics.getProcessCompletedCount(since)).isZero();
    assertThat(metrics.getProcessFailedCount(since)).isZero();
    assertThat(metrics.getProcessExceptionCount(since)).isZero();
    assertThat(metrics.getProcessCreationFailedCount(since)).isZero();
    assertThat(metrics.getStageSuccessCount(since)).isZero();
    assertThat(metrics.getStageFailedCount(since)).isZero();

    ProcessRunnerResult result = new ProcessRunnerResult();
    result.addStageSuccessCount(5L);
    result.addStageFailedCount(10L);
    result.setProcessExceptionCount(15);
    result.setProcessExecutionCount(20);
    metrics.addProcessRunnerResult(ProcessState.FAILED, result);

    assertThat(metrics.getCompletedProcessCount()).isZero();
    assertThat(metrics.getFailedProcessCount()).isEqualTo(20);
    assertThat(metrics.getProcessExceptionCount()).isEqualTo(15);
    assertThat(metrics.getProcessCreationFailedCount()).isZero();
    assertThat(metrics.getSuccessfulStageCount()).isEqualTo(5);
    assertThat(metrics.getFailedStageCount()).isEqualTo(10);

    assertThat(metrics.getProcessCompletedCount(since)).isZero();
    assertThat(metrics.getProcessFailedCount(since)).isEqualTo(20);
    assertThat(metrics.getProcessExceptionCount(since)).isEqualTo(15);
    assertThat(metrics.getProcessCreationFailedCount(since)).isZero();
    assertThat(metrics.getStageSuccessCount(since)).isEqualTo(5);
    assertThat(metrics.getStageFailedCount(since)).isEqualTo(10);

    Duration now = Duration.ofMinutes(0);

    assertThat(metrics.getProcessCompletedCount(now)).isZero();
    assertThat(metrics.getProcessFailedCount(now)).isZero();
    assertThat(metrics.getProcessExceptionCount(now)).isZero();
    assertThat(metrics.getProcessCreationFailedCount(now)).isZero();
    assertThat(metrics.getStageSuccessCount(now)).isZero();
    assertThat(metrics.getStageFailedCount(now)).isZero();

    metrics.purge(now);

    assertThat(metrics.getCompletedProcessCount()).isZero();
    assertThat(metrics.getFailedProcessCount()).isEqualTo(20);
    assertThat(metrics.getProcessExceptionCount()).isEqualTo(15);
    assertThat(metrics.getProcessCreationFailedCount()).isZero();
    assertThat(metrics.getSuccessfulStageCount()).isEqualTo(5);
    assertThat(metrics.getFailedStageCount()).isEqualTo(10);

    assertThat(metrics.getProcessCompletedCount(since)).isZero();
    assertThat(metrics.getProcessFailedCount(since)).isZero();
    assertThat(metrics.getProcessExceptionCount(since)).isZero();
    assertThat(metrics.getProcessCreationFailedCount(since)).isZero();
    assertThat(metrics.getStageSuccessCount(since)).isZero();
    assertThat(metrics.getStageFailedCount(since)).isZero();
  }
}
