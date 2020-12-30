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
import java.time.ZonedDateTime;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import lombok.extern.flogger.Flogger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import pipelite.launcher.process.runner.ProcessRunner;
import tech.tablesaw.api.Table;

@Component
@Flogger
public class PipeliteMetrics {

  private final MeterRegistry meterRegistry;
  private final Map<String, PipelineMetrics> pipelineMetrics = new ConcurrentHashMap<>();

  // Micrometer counters.

  private final Counter internalErrorCounter;

  // Time series.

  private final Table internalErrorTimeSeries;

  public PipeliteMetrics(@Autowired MeterRegistry meterRegistry) {
    this.meterRegistry = meterRegistry;
    internalErrorCounter = meterRegistry.counter("pipelite.error");
    internalErrorTimeSeries = TimeSeriesMetrics.getEmptyTimeSeries("internal errors");
  }

  public PipelineMetrics pipeline(String pipelineName) {
    PipelineMetrics m = pipelineMetrics.get(pipelineName);
    if (m == null) {
      pipelineMetrics.putIfAbsent(pipelineName, new PipelineMetrics(pipelineName, meterRegistry));
      m = pipelineMetrics.get(pipelineName);
    }
    return m;
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

  public void incrementInternalErrorCount() {
    incrementInternalErrorCount(ZonedDateTime.now());
  }

  public void incrementInternalErrorCount(ZonedDateTime now) {
    internalErrorCounter.increment(1);
    TimeSeriesMetrics.updateCounter(internalErrorTimeSeries, 1, now);
  }

  /**
   * Set the number of running processes.
   *
   * @param processRunners the active process runners
   */
  public void setRunningProcessesCount(Collection<ProcessRunner> processRunners) {
    setRunningProcessesCount(processRunners, ZonedDateTime.now());
  }

  /**
   * Set the number of running processes.
   *
   * @param processRunners the active process runners
   * @param now the current time
   */
  public void setRunningProcessesCount(
      Collection<ProcessRunner> processRunners, ZonedDateTime now) {
    Map<String, Integer> counts = new HashMap<>();
    processRunners.forEach(
        r -> {
          Integer count = counts.get(r.getPipelineName());
          if (count != null) {
            counts.put(r.getPipelineName(), count + 1);
          } else {
            counts.put(r.getPipelineName(), 1);
          }
        });
    counts.forEach(
        (pipelineName, count) -> pipeline(pipelineName).process().setRunningCount(count, now));
  }
}
