package pipelite.launcher.process.runner;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import pipelite.process.ProcessState;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

public class ProcessRunnerStatsTest {

  @Test
  public void addProcessCreationFailed() {
    ProcessRunnerStats stats = new ProcessRunnerStats("PIPELINE_NAME", new SimpleMeterRegistry());

    assertThat(stats.getCompletedProcessCount()).isZero();
    assertThat(stats.getFailedProcessCount()).isZero();
    assertThat(stats.getProcessExceptionCount()).isZero();
    assertThat(stats.getProcessCreationFailedCount()).isZero();
    assertThat(stats.getSuccessfulStageCount()).isZero();
    assertThat(stats.getFailedStageCount()).isZero();

    Duration since = Duration.ofDays(1);

    assertThat(stats.getProcessCompletedCount(since)).isZero();
    assertThat(stats.getProcessFailedCount(since)).isZero();
    assertThat(stats.getProcessExceptionCount(since)).isZero();
    assertThat(stats.getProcessCreationFailedCount(since)).isZero();
    assertThat(stats.getStageSuccessCount(since)).isZero();
    assertThat(stats.getStageFailedCount(since)).isZero();

    stats.addProcessCreationFailed(10);

    assertThat(stats.getCompletedProcessCount()).isZero();
    assertThat(stats.getFailedProcessCount()).isZero();
    assertThat(stats.getProcessExceptionCount()).isZero();
    assertThat(stats.getProcessCreationFailedCount()).isEqualTo(10);
    assertThat(stats.getSuccessfulStageCount()).isZero();
    assertThat(stats.getFailedStageCount()).isZero();

    assertThat(stats.getProcessCompletedCount(since)).isZero();
    assertThat(stats.getProcessFailedCount(since)).isZero();
    assertThat(stats.getProcessExceptionCount(since)).isZero();
    assertThat(stats.getProcessCreationFailedCount(since)).isEqualTo(10);
    assertThat(stats.getStageSuccessCount(since)).isZero();
    assertThat(stats.getStageFailedCount(since)).isZero();

    Duration now = Duration.ofMinutes(0);

    assertThat(stats.getProcessCompletedCount(now)).isZero();
    assertThat(stats.getProcessFailedCount(now)).isZero();
    assertThat(stats.getProcessExceptionCount(now)).isZero();
    assertThat(stats.getProcessCreationFailedCount(now)).isZero();
    assertThat(stats.getStageSuccessCount(now)).isZero();
    assertThat(stats.getStageFailedCount(now)).isZero();

    stats.purge(now);

    assertThat(stats.getCompletedProcessCount()).isZero();
    assertThat(stats.getFailedProcessCount()).isZero();
    assertThat(stats.getProcessExceptionCount()).isZero();
    assertThat(stats.getProcessCreationFailedCount()).isEqualTo(10);
    assertThat(stats.getSuccessfulStageCount()).isZero();
    assertThat(stats.getFailedStageCount()).isZero();

    assertThat(stats.getProcessCompletedCount(since)).isZero();
    assertThat(stats.getProcessFailedCount(since)).isZero();
    assertThat(stats.getProcessExceptionCount(since)).isZero();
    assertThat(stats.getProcessCreationFailedCount(since)).isZero();
    assertThat(stats.getStageSuccessCount(since)).isZero();
    assertThat(stats.getStageFailedCount(since)).isZero();
  }

  @Test
  public void addCompletedProcessRunnerResult() {
    ProcessRunnerStats stats = new ProcessRunnerStats("PIPELINE_NAME", new SimpleMeterRegistry());

    assertThat(stats.getCompletedProcessCount()).isZero();
    assertThat(stats.getFailedProcessCount()).isZero();
    assertThat(stats.getProcessExceptionCount()).isZero();
    assertThat(stats.getProcessCreationFailedCount()).isZero();
    assertThat(stats.getSuccessfulStageCount()).isZero();
    assertThat(stats.getFailedStageCount()).isZero();

    Duration since = Duration.ofDays(1);

    assertThat(stats.getProcessCompletedCount(since)).isZero();
    assertThat(stats.getProcessFailedCount(since)).isZero();
    assertThat(stats.getProcessExceptionCount(since)).isZero();
    assertThat(stats.getProcessCreationFailedCount(since)).isZero();
    assertThat(stats.getStageSuccessCount(since)).isZero();
    assertThat(stats.getStageFailedCount(since)).isZero();

    ProcessRunnerResult result = new ProcessRunnerResult();
    result.addStageSuccessCount(5L);
    result.addStageFailedCount(10L);
    result.setProcessExceptionCount(15);
    result.setProcessExecutionCount(20);
    stats.addProcessRunnerResult(ProcessState.COMPLETED, result);

    assertThat(stats.getCompletedProcessCount()).isEqualTo(20);
    assertThat(stats.getFailedProcessCount()).isZero();
    assertThat(stats.getProcessExceptionCount()).isEqualTo(15);
    assertThat(stats.getProcessCreationFailedCount()).isZero();
    assertThat(stats.getSuccessfulStageCount()).isEqualTo(5);
    assertThat(stats.getFailedStageCount()).isEqualTo(10);

    assertThat(stats.getProcessCompletedCount(since)).isEqualTo(20);
    assertThat(stats.getProcessFailedCount(since)).isZero();
    assertThat(stats.getProcessExceptionCount(since)).isEqualTo(15);
    assertThat(stats.getProcessCreationFailedCount(since)).isZero();
    assertThat(stats.getStageSuccessCount(since)).isEqualTo(5);
    assertThat(stats.getStageFailedCount(since)).isEqualTo(10);

    Duration now = Duration.ofMinutes(0);

    assertThat(stats.getProcessCompletedCount(now)).isZero();
    assertThat(stats.getProcessFailedCount(now)).isZero();
    assertThat(stats.getProcessExceptionCount(now)).isZero();
    assertThat(stats.getProcessCreationFailedCount(now)).isZero();
    assertThat(stats.getStageSuccessCount(now)).isZero();
    assertThat(stats.getStageFailedCount(now)).isZero();

    stats.purge(now);

    assertThat(stats.getCompletedProcessCount()).isEqualTo(20);
    assertThat(stats.getFailedProcessCount()).isZero();
    assertThat(stats.getProcessExceptionCount()).isEqualTo(15);
    assertThat(stats.getProcessCreationFailedCount()).isZero();
    assertThat(stats.getSuccessfulStageCount()).isEqualTo(5);
    assertThat(stats.getFailedStageCount()).isEqualTo(10);

    assertThat(stats.getProcessCompletedCount(since)).isZero();
    assertThat(stats.getProcessFailedCount(since)).isZero();
    assertThat(stats.getProcessExceptionCount(since)).isZero();
    assertThat(stats.getProcessCreationFailedCount(since)).isZero();
    assertThat(stats.getStageSuccessCount(since)).isZero();
    assertThat(stats.getStageFailedCount(since)).isZero();
  }

  @Test
  public void addFailedProcessRunnerResult() {
    ProcessRunnerStats stats = new ProcessRunnerStats("PIPELINE_NAME", new SimpleMeterRegistry());

    assertThat(stats.getCompletedProcessCount()).isZero();
    assertThat(stats.getFailedProcessCount()).isZero();
    assertThat(stats.getProcessExceptionCount()).isZero();
    assertThat(stats.getProcessCreationFailedCount()).isZero();
    assertThat(stats.getSuccessfulStageCount()).isZero();
    assertThat(stats.getFailedStageCount()).isZero();

    Duration since = Duration.ofDays(1);

    assertThat(stats.getProcessCompletedCount(since)).isZero();
    assertThat(stats.getProcessFailedCount(since)).isZero();
    assertThat(stats.getProcessExceptionCount(since)).isZero();
    assertThat(stats.getProcessCreationFailedCount(since)).isZero();
    assertThat(stats.getStageSuccessCount(since)).isZero();
    assertThat(stats.getStageFailedCount(since)).isZero();

    ProcessRunnerResult result = new ProcessRunnerResult();
    result.addStageSuccessCount(5L);
    result.addStageFailedCount(10L);
    result.setProcessExceptionCount(15);
    result.setProcessExecutionCount(20);
    stats.addProcessRunnerResult(ProcessState.FAILED, result);

    assertThat(stats.getCompletedProcessCount()).isZero();
    assertThat(stats.getFailedProcessCount()).isEqualTo(20);
    assertThat(stats.getProcessExceptionCount()).isEqualTo(15);
    assertThat(stats.getProcessCreationFailedCount()).isZero();
    assertThat(stats.getSuccessfulStageCount()).isEqualTo(5);
    assertThat(stats.getFailedStageCount()).isEqualTo(10);

    assertThat(stats.getProcessCompletedCount(since)).isZero();
    assertThat(stats.getProcessFailedCount(since)).isEqualTo(20);
    assertThat(stats.getProcessExceptionCount(since)).isEqualTo(15);
    assertThat(stats.getProcessCreationFailedCount(since)).isZero();
    assertThat(stats.getStageSuccessCount(since)).isEqualTo(5);
    assertThat(stats.getStageFailedCount(since)).isEqualTo(10);

    Duration now = Duration.ofMinutes(0);

    assertThat(stats.getProcessCompletedCount(now)).isZero();
    assertThat(stats.getProcessFailedCount(now)).isZero();
    assertThat(stats.getProcessExceptionCount(now)).isZero();
    assertThat(stats.getProcessCreationFailedCount(now)).isZero();
    assertThat(stats.getStageSuccessCount(now)).isZero();
    assertThat(stats.getStageFailedCount(now)).isZero();

    stats.purge(now);

    assertThat(stats.getCompletedProcessCount()).isZero();
    assertThat(stats.getFailedProcessCount()).isEqualTo(20);
    assertThat(stats.getProcessExceptionCount()).isEqualTo(15);
    assertThat(stats.getProcessCreationFailedCount()).isZero();
    assertThat(stats.getSuccessfulStageCount()).isEqualTo(5);
    assertThat(stats.getFailedStageCount()).isEqualTo(10);

    assertThat(stats.getProcessCompletedCount(since)).isZero();
    assertThat(stats.getProcessFailedCount(since)).isZero();
    assertThat(stats.getProcessExceptionCount(since)).isZero();
    assertThat(stats.getProcessCreationFailedCount(since)).isZero();
    assertThat(stats.getStageSuccessCount(since)).isZero();
    assertThat(stats.getStageFailedCount(since)).isZero();
  }
}
