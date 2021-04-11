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

import com.google.common.util.concurrent.AtomicDouble;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.ZonedDateTime;
import tech.tablesaw.api.Table;

public class ProcessMetrics {

  // Micrometer counters.

  private final AtomicDouble runningGauge = new AtomicDouble();
  private final Counter completedCounter;
  private final Counter failedCounter;
  private final Counter internalErrorCounter;

  // Time series.

  private final Table runningTimeSeries;
  private final Table completedTimeSeries;
  private final Table failedTimeSeries;
  private final Table internalErrorTimeSeries;

  public ProcessMetrics(String pipelineName, MeterRegistry meterRegistry) {
    Gauge.builder("pipelite.process.running", runningGauge, AtomicDouble::get)
        .tags("pipelineName", pipelineName)
        .register(meterRegistry);
    completedCounter =
        meterRegistry.counter("pipelite.process.completed", "pipelineName", pipelineName);
    failedCounter = meterRegistry.counter("pipelite.process.failed", "pipelineName", pipelineName);
    internalErrorCounter =
        meterRegistry.counter("pipelite.process.error", "pipelineName", pipelineName);

    runningTimeSeries = TimeSeriesMetrics.getEmptyTimeSeries(pipelineName);
    completedTimeSeries = TimeSeriesMetrics.getEmptyTimeSeries(pipelineName);
    failedTimeSeries = TimeSeriesMetrics.getEmptyTimeSeries(pipelineName);
    internalErrorTimeSeries = TimeSeriesMetrics.getEmptyTimeSeries(pipelineName);
  }

  public double getRunningCount() {
    return runningGauge.get();
  }

  public double getCompletedCount() {
    return completedCounter.count();
  }

  public double getFailedCount() {
    return failedCounter.count();
  }

  public double getInternalErrorCount() {
    return internalErrorCounter.count();
  }

  public Table getRunningTimeSeries() {
    return runningTimeSeries;
  }

  public Table getCompletedTimeSeries() {
    return completedTimeSeries;
  }

  public Table getFailedTimeSeries() {
    return failedTimeSeries;
  }

  public Table getInternalErrorTimeSeries() {
    return internalErrorTimeSeries;
  }

  public void setRunningCount(int count, ZonedDateTime now) {
    runningGauge.set(count);
    TimeSeriesMetrics.updateGauge(runningTimeSeries, count, now);
  }

  public void incrementCompletedCount(ZonedDateTime now) {
    completedCounter.increment(1);
    TimeSeriesMetrics.updateCounter(completedTimeSeries, 1, now);
  }

  public void incrementFailedCount(ZonedDateTime now) {
    failedCounter.increment(1);
    TimeSeriesMetrics.updateCounter(failedTimeSeries, 1, now);
  }

  public void incrementInternalErrorCount() {
    internalErrorCounter.increment(1);
    TimeSeriesMetrics.updateCounter(internalErrorTimeSeries, 1, ZonedDateTime.now());
  }
}
