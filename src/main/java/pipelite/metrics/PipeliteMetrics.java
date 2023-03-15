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
package pipelite.metrics;

import io.micrometer.core.instrument.MeterRegistry;
import java.time.ZonedDateTime;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import lombok.extern.flogger.Flogger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import pipelite.RegisteredPipeline;
import pipelite.exception.PipeliteException;
import pipelite.metrics.collector.InternalErrorMetrics;
import pipelite.runner.process.ProcessRunner;

@Component
@Flogger
public class PipeliteMetrics {

  private final MeterRegistry meterRegistry;
  private final InternalErrorMetrics internalErrorMetrics;
  private final Map<String, ProcessMetrics> process = new ConcurrentHashMap<>();
  private final List<RegisteredPipeline> registeredPipelines;

  public PipeliteMetrics(
      @Autowired MeterRegistry meterRegistry,
      @Autowired List<RegisteredPipeline> registeredPipelines) {
    this.meterRegistry = meterRegistry;
    this.internalErrorMetrics = new InternalErrorMetrics(meterRegistry);
    this.registeredPipelines = registeredPipelines;
  }

  public InternalErrorMetrics error() {
    return internalErrorMetrics;
  }

  public ProcessMetrics process(String pipelineName) {
    if (pipelineName == null) {
      throw new PipeliteException("Missing pipeline name");
    }
    try {
      ProcessMetrics m = process.get(pipelineName);
      if (m == null) {
        process.putIfAbsent(pipelineName, new ProcessMetrics(pipelineName, meterRegistry));
        return process.get(pipelineName);
      }
      return m;
    } catch (Exception ex) {
      process.putIfAbsent(pipelineName, new ProcessMetrics(pipelineName, meterRegistry));
      return process.get(pipelineName);
    }
  }

  /**
   * Set the number of running processes.
   *
   * @param processRunners the active process runners
   */
  public void setRunningProcessesCount(
      Collection<ProcessRunner> processRunners, ZonedDateTime now) {
    Map<String, Integer> counts = new HashMap<>();
    registeredPipelines.forEach(r -> counts.put(r.pipelineName(), 0));
    processRunners.forEach(r -> counts.merge(r.getPipelineName(), 1, Integer::sum));
    counts.forEach(
        (pipelineName, count) ->
            process(pipelineName).runner().setRunningProcessesCount(count, now));
  }

  /**
   * Set the number of runnin stages.
   *
   * @param processRunners the active process runners
   */
  public void setRunningStagesCount(Collection<ProcessRunner> processRunners, ZonedDateTime now) {
    Map<String, Integer> counts = new HashMap<>();
    registeredPipelines.forEach(r -> counts.put(r.pipelineName(), 0));
    for (ProcessRunner processRunner : processRunners) {
      processRunner
          .activeStages()
          .forEach(r -> counts.merge(processRunner.getPipelineName(), 1, Integer::sum));
    }
    counts.forEach(
        (pipelineName, count) -> process(pipelineName).runner().setRunningStagesCount(count, now));
  }

  /**
   * Set the number of submitted stages.
   *
   * @param processRunners the active process runners
   */
  public void setSubmittedStagesCount(Collection<ProcessRunner> processRunners, ZonedDateTime now) {
    Map<String, Integer> counts = new HashMap<>();
    registeredPipelines.forEach(r -> counts.put(r.pipelineName(), 0));
    for (ProcessRunner processRunner : processRunners) {
      processRunner
          .submittedStages()
          .forEach(r -> counts.merge(processRunner.getPipelineName(), 1, Integer::sum));
    }
    counts.forEach(
        (pipelineName, count) ->
            process(pipelineName).runner().setSubmittedStagesCount(count, now));
  }
}
