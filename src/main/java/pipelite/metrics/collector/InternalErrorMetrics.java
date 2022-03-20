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
package pipelite.metrics.collector;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.ZonedDateTime;
import pipelite.metrics.helper.TimeSeriesHelper;
import tech.tablesaw.api.Table;

public class InternalErrorMetrics extends AbstractMetrics {

  private static final String PREFIX = "pipelite.error";

  // Micrometer
  private final Counter counter;

  // Tablesaw time series
  private final Table timeSeries;

  public InternalErrorMetrics(MeterRegistry meterRegistry) {
    super(PREFIX);
    this.counter = meterRegistry.counter(name("internal"));
    this.timeSeries = TimeSeriesHelper.getEmptyTimeSeries("internal errors");
  }

  public double count() {
    return counter.count();
  }

  public Table timeSeries() {
    return timeSeries;
  }

  /** Called from InternalErrorService. */
  public void incrementCount() {
    counter.increment(1);
    TimeSeriesHelper.incrementTimeSeriesCount(timeSeries, 1, "error", ZonedDateTime.now());
  }
}
