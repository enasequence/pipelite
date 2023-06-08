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
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.flogger.Flogger;
import pipelite.metrics.helper.MicroMeterHelper;

@Flogger
public class ProcessCountMetrics extends AbstractMetrics {

  private static final String PREFIX = "pipelite.process";
  private final String pipelineName;

  // Micrometer metrics
  private final AtomicDouble backlogGauge = new AtomicDouble();
  private final AtomicDouble failedGauge = new AtomicDouble();

  public ProcessCountMetrics(String pipelineName, MeterRegistry meterRegistry) {
    super(PREFIX);
    this.pipelineName = pipelineName;
    String[] backlogTags = MicroMeterHelper.pipelineTags(pipelineName);
    Gauge.builder(name("backlog"), () -> backlogGauge.get())
        .tags(backlogTags)
        .register(meterRegistry);
    Gauge.builder(name("failed"), () -> failedGauge.get())
        .tags(backlogTags)
        .register(meterRegistry);
  }

  /**
   * Set the number of pending and active processes.
   *
   * @param count the number of pending and active processes.
   */
  public void setBacklogCount(int count) {
    log.atFiner().log("Backlog process count for " + pipelineName + ": " + count);
    backlogGauge.set(count);
  }

  /**
   * Set the number of failed processes.
   *
   * @param count the number of failes processes
   */
  public void setFailedCount(int count) {
    log.atFiner().log("Failed process count for " + pipelineName + ": " + count);
    failedGauge.set(count);
  }
}
