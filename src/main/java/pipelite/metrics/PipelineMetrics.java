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

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import pipelite.launcher.process.runner.ProcessRunnerResult;
import pipelite.process.ProcessState;
import tech.tablesaw.api.Table;

import java.time.LocalDateTime;
import java.time.ZonedDateTime;

public class PipelineMetrics {

  private final ProcessMetrics processMetrics;
  private final StageMetrics stageMetrics;

  // Micrometer counters.

  private final Counter internalErrorCounter;

  // Time series.

  private final Table internalErrorTimeSeries;

  public PipelineMetrics(String pipelineName, MeterRegistry meterRegistry) {
    processMetrics = new ProcessMetrics(pipelineName, meterRegistry);
    stageMetrics = new StageMetrics(pipelineName, meterRegistry);
    internalErrorCounter = meterRegistry.counter("pipelite.error");
    internalErrorTimeSeries = TimeSeriesMetrics.getEmptyTimeSeries(pipelineName);
  }

  public ProcessMetrics process() {
    return processMetrics;
  }

  public StageMetrics stage() {
    return stageMetrics;
  }

  public void increment(ProcessState state, ProcessRunnerResult result) {
    increment(state, result, ZonedDateTime.now());
  }

  public void increment(ProcessState state, ProcessRunnerResult result, ZonedDateTime now) {
    if (state == ProcessState.COMPLETED) {
      processMetrics.incrementCompletedCount(now);
    }
    if (state == ProcessState.FAILED) {
      processMetrics.incrementFailedCount(now);
    }
    stageMetrics.increment(result, now);
    incrementInternalErrorCount(result.getInternalErrorCount());
  }

  public double getInternalErrorCount() {
    return internalErrorCounter.count();
  }

  public double getInternalErrorCount(ZonedDateTime since) {
    return TimeSeriesMetrics.getCount(internalErrorTimeSeries, since);
  }

  public Table getInternalErrorTimeSeries() {
    return internalErrorTimeSeries;
  }

  /** Increment internal error count. */
  public void incrementInternalErrorCount() {
    incrementInternalErrorCount(1, ZonedDateTime.now());
  }

  /**
   * Increment internal error count.
   *
   * @param count the number of internal errors
   */
  public void incrementInternalErrorCount(long count) {
    incrementInternalErrorCount(count, ZonedDateTime.now());
  }

  public void incrementInternalErrorCount(long count, ZonedDateTime now) {
    internalErrorCounter.increment(count);
    TimeSeriesMetrics.updateCounter(internalErrorTimeSeries, count, now);
  }
}
