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

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;

public class InternalErrorMetrics extends AbstractMetrics {

  private static final String PREFIX = "pipelite.error";

  // Micrometer
  private final Counter counter;

  public InternalErrorMetrics(MeterRegistry meterRegistry) {
    super(PREFIX);
    this.counter = meterRegistry.counter(name("internal"));
  }

  public double count() {
    return counter.count();
  }

  /** Called from InternalErrorService. */
  public void incrementCount() {
    counter.increment(1);
  }
}
