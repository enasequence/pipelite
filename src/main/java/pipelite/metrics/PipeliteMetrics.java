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
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import lombok.extern.flogger.Flogger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import pipelite.exception.PipeliteException;
import pipelite.runner.process.ProcessRunner;
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

  // Micrometer timers.

  private final Timer processRunnerPoolOneIterationTimer;
  private final Timer processRunnerOneIterationTimer;
  private final Timer stageRunnerOneIterationTimer;

  public PipeliteMetrics(@Autowired MeterRegistry meterRegistry) {
    this.meterRegistry = meterRegistry;
    this.internalErrorCounter = meterRegistry.counter("pipelite.error");
    this.internalErrorTimeSeries = TimeSeriesMetrics.getEmptyTimeSeries("internal errors");
    this.processRunnerPoolOneIterationTimer =
        meterRegistry.timer("pipelite.processRunnerPool.runOneIteration");
    this.processRunnerOneIterationTimer =
        meterRegistry.timer("pipelite.processRunner.runOneIteration");
    this.stageRunnerOneIterationTimer = meterRegistry.timer("pipelite.stageRunner.runOneIteration");
  }

  public PipelineMetrics pipeline(String pipelineName) {
    if (pipelineName == null) {
      throw new PipeliteException("Missing pipeline name");
    }
    try {
      PipelineMetrics m = pipelineMetrics.get(pipelineName);
      if (m == null) {
        pipelineMetrics.putIfAbsent(pipelineName, new PipelineMetrics(pipelineName, meterRegistry));
        return pipelineMetrics.get(pipelineName);
      }
      return m;
    } catch (Exception ex) {
      pipelineMetrics.putIfAbsent(pipelineName, new PipelineMetrics(pipelineName, meterRegistry));
      return pipelineMetrics.get(pipelineName);
    }
  }

  public double getInternalErrorCount() {
    return internalErrorCounter.count();
  }

  public Table getInternalErrorTimeSeries() {
    return internalErrorTimeSeries;
  }

  public Timer getProcessRunnerPoolOneIterationTimer() {
    return processRunnerPoolOneIterationTimer;
  }

  public Timer getProcessRunnerOneIterationTimer() {
    return processRunnerOneIterationTimer;
  }

  public Timer getStageRunnerOneIterationTimer() {
    return stageRunnerOneIterationTimer;
  }

  /** Called from InternalErrorService. */
  public void incrementInternalErrorCount() {
    internalErrorCounter.increment(1);
    TimeSeriesMetrics.updateCounter(internalErrorTimeSeries, 1, ZonedDateTime.now());
  }

  /**
   * Set the number of running processes.
   *
   * @param processRunners the active process runners
   */
  public void setRunningProcessesCount(Collection<ProcessRunner> processRunners) {
    Map<String, Integer> counts = new HashMap<>();
    processRunners.forEach(r -> counts.merge(r.getPipelineName(), 1, Integer::sum));
    counts.forEach(
        (pipelineName, count) -> pipeline(pipelineName).process().setRunningProcessesCount(count));
  }
}
