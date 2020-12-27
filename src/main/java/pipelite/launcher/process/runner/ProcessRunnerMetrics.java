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

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Duration;
import pipelite.process.ProcessState;

public class ProcessRunnerMetrics {

  private static final Duration MAX_WINDOW = Duration.ofHours(24);

  private final Counter completedProcessCount;
  private final Counter failedProcessCount;
  private final Counter processExceptionCount;
  private final Counter processCreationFailedCount;
  private final Counter successfulStageCount;
  private final Counter failedStageCount;
  private final Counter stageExceptionCount;

  private final ProcessRunnerCounter completedProcessWindowCount = new ProcessRunnerCounter();
  private final ProcessRunnerCounter failedProcessWindowCount = new ProcessRunnerCounter();
  private final ProcessRunnerCounter processExceptionWindowCount = new ProcessRunnerCounter();
  private final ProcessRunnerCounter processCreationFailedWindowCount = new ProcessRunnerCounter();
  private final ProcessRunnerCounter successfulStageWindowCount = new ProcessRunnerCounter();
  private final ProcessRunnerCounter failedStageWindowCount = new ProcessRunnerCounter();
  private final ProcessRunnerCounter stageExceptionWindowCount = new ProcessRunnerCounter();

  public ProcessRunnerMetrics(String pipelineName, MeterRegistry meterRegistry) {
    completedProcessCount =
        meterRegistry.counter("pipelite.process.completed", "pipelineName", pipelineName);
    failedProcessCount =
        meterRegistry.counter("pipelite.process.failed", "pipelineName", pipelineName);
    processExceptionCount =
        meterRegistry.counter("pipelite.process.exception", "pipelineName", pipelineName);
    processCreationFailedCount =
        meterRegistry.counter("pipelite.process.creationFailed", "pipelineName", pipelineName);
    failedStageCount = meterRegistry.counter("pipelite.stage.failed", "pipelineName", pipelineName);
    successfulStageCount =
        meterRegistry.counter("pipelite.stage.success", "pipelineName", pipelineName);
    stageExceptionCount =
        meterRegistry.counter("pipelite.stage.exception", "pipelineName", pipelineName);
  }

  public double getCompletedProcessCount() {
    return completedProcessCount.count();
  }

  public double getFailedProcessCount() {
    return failedProcessCount.count();
  }

  public double getProcessExceptionCount() {
    return processExceptionCount.count();
  }

  public double getProcessCreationFailedCount() {
    return processCreationFailedCount.count();
  }

  public double getSuccessfulStageCount() {
    return successfulStageCount.count();
  }

  public double getFailedStageCount() {
    return failedStageCount.count();
  }

  public double getProcessCompletedCount(Duration since) {
    return completedProcessWindowCount.getCount(since);
  }

  public double getProcessFailedCount(Duration since) {
    return failedProcessWindowCount.getCount(since);
  }

  public double getProcessExceptionCount(Duration since) {
    return processExceptionWindowCount.getCount(since);
  }

  public double getProcessCreationFailedCount(Duration since) {
    return processCreationFailedWindowCount.getCount(since);
  }

  public double getStageSuccessCount(Duration since) {
    return successfulStageWindowCount.getCount(since);
  }

  public double getStageFailedCount(Duration since) {
    return failedStageWindowCount.getCount(since);
  }

  public double getStageExceptionCount(Duration since) {
    return failedStageWindowCount.getCount(since);
  }

  public void addProcessCreationFailed(double count) {
    processCreationFailedCount.increment(count);
    processCreationFailedWindowCount.increment(count);
  }

  public void addProcessRunnerResult(ProcessState state, ProcessRunnerResult result) {
    if (result.getProcessExecutionCount() > 0) {
      if (state == ProcessState.COMPLETED) {
        completedProcessCount.increment(result.getProcessExecutionCount());
        completedProcessWindowCount.increment(result.getProcessExecutionCount());
      }
      if (state == ProcessState.FAILED) {
        failedProcessCount.increment(result.getProcessExecutionCount());
        failedProcessWindowCount.increment(result.getProcessExecutionCount());
      }
    }
    processExceptionCount.increment(result.getProcessExceptionCount());
    processExceptionWindowCount.increment(result.getProcessExceptionCount());
    successfulStageCount.increment(result.getStageSuccessCount());
    successfulStageWindowCount.increment(result.getStageSuccessCount());
    failedStageCount.increment(result.getStageFailedCount());
    failedStageWindowCount.increment(result.getStageFailedCount());
    stageExceptionCount.increment(result.getStageExceptionCount());
    stageExceptionWindowCount.increment(result.getStageExceptionCount());
  }

  public void purge() {
    purge(MAX_WINDOW);
  }

  public void purge(Duration since) {
    completedProcessWindowCount.purge(since);
    failedProcessWindowCount.purge(since);
    processExceptionWindowCount.purge(since);
    processCreationFailedWindowCount.purge(since);
    successfulStageWindowCount.purge(since);
    failedStageWindowCount.purge(since);
    stageExceptionWindowCount.purge(since);
  }
}
