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
package pipelite.metrics.collector;

import com.google.common.util.concurrent.AtomicDouble;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.ZonedDateTime;
import lombok.extern.flogger.Flogger;
import pipelite.entity.field.StageState;
import pipelite.metrics.helper.MicroMeterHelper;
import pipelite.metrics.helper.TimeSeriesHelper;
import pipelite.process.ProcessState;
import tech.tablesaw.api.Table;

@Flogger
public class ProcessRunnerMetrics extends AbstractMetrics {

  private static final String PREFIX = "pipelite.process";

  private final String pipelineName;

  // Micrometer
  private final AtomicDouble runningGauge = new AtomicDouble();
  private final Counter completedCounter;
  private final Counter failedCounter;

  // Tablesaw time series
  private final Table runningTimeSeries;
  private final Table completedTimeSeries;
  private final Table failedTimeSeries;
  private final Table runningStagesTimeSeries;
  private final Table completedStagesTimeSeries;
  private final Table failedStagesTimeSeries;

  public ProcessRunnerMetrics(String pipelineName, MeterRegistry meterRegistry) {
    super(PREFIX);
    this.pipelineName = pipelineName;
    String[] tags = MicroMeterHelper.pipelineTags(pipelineName);

    Gauge.builder(name("running"), runningGauge, AtomicDouble::get)
        .tags(tags)
        .strongReference(true) // prevent garbage collection.
        .register(meterRegistry);

    completedCounter = meterRegistry.counter(name("completed"), tags);
    failedCounter = meterRegistry.counter(name("failed"), tags);

    runningTimeSeries = TimeSeriesHelper.getEmptyTimeSeries(pipelineName);
    completedTimeSeries = TimeSeriesHelper.getEmptyTimeSeries(pipelineName);
    failedTimeSeries = TimeSeriesHelper.getEmptyTimeSeries(pipelineName);
    runningStagesTimeSeries = TimeSeriesHelper.getEmptyTimeSeries(pipelineName);
    completedStagesTimeSeries = TimeSeriesHelper.getEmptyTimeSeries(pipelineName);
    failedStagesTimeSeries = TimeSeriesHelper.getEmptyTimeSeries(pipelineName);
  }

  public double completedCount() {
    return completedCounter.count();
  }

  public double failedCount() {
    return failedCounter.count();
  }

  public Table runningTimeSeries() {
    return runningTimeSeries;
  }

  public Table completedTimeSeries() {
    return completedTimeSeries;
  }

  public Table failedTimeSeries() {
    return failedTimeSeries;
  }

  public Table runningStagesTimeSeries() {
    return runningStagesTimeSeries;
  }

  public Table completedStagesTimeSeries() {
    return completedStagesTimeSeries;
  }

  public Table failedStagesTimeSeries() {
    return failedStagesTimeSeries;
  }

  /**
   * Set the number of running processes.
   *
   * @param count the number of running processes
   * @paran now the time when the running process count was measured
   */
  public void setRunningProcessesCount(int count, ZonedDateTime now) {
    log.atFiner().log("Running processes count for " + pipelineName + ": " + count);
    runningGauge.set(count);
    TimeSeriesHelper.maximumTimeSeriesCount(runningTimeSeries, count, pipelineName, now);
  }

  /**
   * Set the number of running stages.
   *
   * @param count the number of running stages
   * @paran now the time when the running stage count was measured
   */
  public void setRunningStagesCount(int count, ZonedDateTime now) {
    log.atFiner().log("Running stages count for " + pipelineName + ": " + count);
    TimeSeriesHelper.maximumTimeSeriesCount(runningStagesTimeSeries, count, pipelineName, now);
  }

  public void endProcessExecution(ProcessState state) {
    endProcessExecution(state, ZonedDateTime.now());
  }

  public void endProcessExecution(ProcessState state, ZonedDateTime now) {
    if (state == ProcessState.COMPLETED) {
      completedCounter.increment(1);
      TimeSeriesHelper.incrementTimeSeriesCount(completedTimeSeries, 1, pipelineName, now);
    } else if (state == ProcessState.FAILED) {
      failedCounter.increment(1);
      TimeSeriesHelper.incrementTimeSeriesCount(failedTimeSeries, 1, pipelineName, now);
    }
  }

  public void endStageExecution(StageState state) {
    endStageExecution(state, ZonedDateTime.now());
  }

  public void endStageExecution(StageState state, ZonedDateTime now) {
    if (state == StageState.SUCCESS) {
      TimeSeriesHelper.incrementTimeSeriesCount(completedStagesTimeSeries, 1, pipelineName, now);
    } else if (state == StageState.ERROR) {
      TimeSeriesHelper.incrementTimeSeriesCount(failedStagesTimeSeries, 1, pipelineName, now);
    }
  }
}
