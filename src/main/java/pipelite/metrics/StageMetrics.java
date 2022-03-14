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
import io.micrometer.core.instrument.Timer;
import java.time.ZonedDateTime;
import pipelite.stage.executor.StageExecutorResult;
import tech.tablesaw.api.Table;

public class StageMetrics {

  // Micrometer counters.

  private final Counter asyncSubmitCounter;
  private final Counter successCounter;
  private final Counter failedCounter;

  // Time series.

  private final Table asyncSubmitTimeSeries;
  private final Table successTimeSeries;
  private final Table failedTimeSeries;

  // Micrometer timers.

  private final Timer asyncSubmitTimer;

  public StageMetrics(String pipelineName, MeterRegistry meterRegistry) {
    asyncSubmitCounter =
        meterRegistry.counter("pipelite.stage.asyncSubmit", "pipelineName", pipelineName);
    failedCounter = meterRegistry.counter("pipelite.stage.failed", "pipelineName", pipelineName);
    successCounter = meterRegistry.counter("pipelite.stage.success", "pipelineName", pipelineName);

    asyncSubmitTimeSeries = TimeSeriesMetrics.getEmptyTimeSeries(pipelineName);
    successTimeSeries = TimeSeriesMetrics.getEmptyTimeSeries(pipelineName);
    failedTimeSeries = TimeSeriesMetrics.getEmptyTimeSeries(pipelineName);

    asyncSubmitTimer = meterRegistry.timer("pipelite.stage.asyncSubmitTimer");
  }

  public double getAsyncSubmitCount() {
    return asyncSubmitCounter.count();
  }

  public double getSuccessCount() {
    return successCounter.count();
  }

  public double getFailedCount() {
    return failedCounter.count();
  }

  public Table getAsyncSubmitTimeSeries() {
    return asyncSubmitTimeSeries;
  }

  public Table getSuccessTimeSeries() {
    return successTimeSeries;
  }

  public Table getFailedTimeSeries() {
    return failedTimeSeries;
  }

  public Timer getAsyncSubmitTimer() {
    return asyncSubmitTimer;
  }

  public void endAsyncSubmit() {
    asyncSubmitCounter.increment(1);
    TimeSeriesMetrics.updateCounter(asyncSubmitTimeSeries, 1, ZonedDateTime.now());
  }

  public void endStageExecution(StageExecutorResult result) {
    if (result.isSuccess()) {
      successCounter.increment(1);
      TimeSeriesMetrics.updateCounter(successTimeSeries, 1, ZonedDateTime.now());
    } else if (result.isError()) {
      failedCounter.increment(1);
      TimeSeriesMetrics.updateCounter(failedTimeSeries, 1, ZonedDateTime.now());
    }
  }
}
