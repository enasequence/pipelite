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
package pipelite.service;

import io.micrometer.core.instrument.MeterRegistry;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import lombok.extern.flogger.Flogger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import pipelite.Pipeline;
import pipelite.RegisteredPipeline;
import pipelite.Schedule;
import pipelite.cron.CronUtils;
import pipelite.exception.PipeliteException;
import pipelite.metrics.collector.ProcessCountMetrics;
import pipelite.repository.ProcessCountRepository;

@Service
@Flogger
public class RegisteredPipelineService {

  private final ProcessCountRepository processCountRepository;
  private final MeterRegistry meterRegistry;
  private final Map<String, RegisteredPipeline> registeredPipelineMap = new HashMap<>();
  private final Map<String, ProcessCountMetrics> processCountMetricsMap = new HashMap<>();

  public RegisteredPipelineService(
      @Autowired List<RegisteredPipeline> registeredPipelines,
      @Autowired ProcessCountRepository processCountRepository,
      @Autowired MeterRegistry meterRegistry) {
    this.processCountRepository = processCountRepository;
    this.meterRegistry = meterRegistry;
    registeredPipelines.forEach(p -> registerPipeline(p));
  }

  public void registerPipeline(RegisteredPipeline registeredPipeline) {
    String pipelineName = registeredPipeline.pipelineName();
    if (pipelineName == null || pipelineName.trim().isEmpty()) {
      throw new PipeliteException("Missing pipeline name");
    }
    if (registeredPipelineMap.containsKey(pipelineName)) {
      throw new PipeliteException("Non-unique pipeline name: " + pipelineName);
    }
    if (registeredPipeline instanceof Schedule) {
      Schedule schedule = (Schedule) registeredPipeline;
      String cron = schedule.configurePipeline().cron();
      if (cron == null) {
        throw new PipeliteException(
            "Missing cron expression for pipeline schedule: " + pipelineName);
      }
      if (!CronUtils.validate(cron)) {
        throw new PipeliteException(
            "Invalid cron expression '" + cron + "' for pipeline schedule: " + pipelineName);
      }
    } else if (registeredPipeline instanceof Pipeline) {
      Pipeline pipeline = (Pipeline) registeredPipeline;
      int pipelineParallelism = pipeline.configurePipeline().pipelineParallelism();
      if (pipelineParallelism < 1) {
        throw new PipeliteException(
            "Invalid pipeline parallelism '"
                + pipelineParallelism
                + "' for pipeline: "
                + pipelineName);
      }
      processCountMetricsMap.put(
          pipelineName, new ProcessCountMetrics(pipelineName, meterRegistry));
    }
    registeredPipelineMap.put(pipelineName, registeredPipeline);
  }

  /**
   * Returns true if one or more schedules are registered.
   *
   * @return true if one or more schedules are registered
   */
  public boolean isSchedules() {
    return registeredPipelineMap.values().stream()
        .filter(s -> s instanceof Schedule)
        .findAny()
        .isPresent();
  }

  /**
   * Returns a registered pipeline.
   *
   * @param pipelineName the pipeline name.
   * @return the registered pipeline.
   * @throws PipeliteException if the pipeline was not found
   */
  public RegisteredPipeline getRegisteredPipeline(String pipelineName) {
    if (pipelineName == null || pipelineName.trim().isEmpty()) {
      throw new PipeliteException("Missing pipeline name");
    }
    if (registeredPipelineMap.containsKey(pipelineName)) {
      return registeredPipelineMap.get(pipelineName);
    }
    throw new PipeliteException("Unknown pipeline: " + pipelineName);
  }

  /**
   * Returns a registered pipeline.
   *
   * @param pipelineName the pipeline name.
   * @return the registered pipeline.
   * @throws PipeliteException if the pipeline was not found
   */
  public <T extends RegisteredPipeline> T getRegisteredPipeline(String pipelineName, Class<T> cls) {
    if (pipelineName == null || pipelineName.trim().isEmpty()) {
      throw new PipeliteException("Missing pipeline name");
    }
    RegisteredPipeline registeredPipeline = registeredPipelineMap.get(pipelineName);
    if (registeredPipeline != null && cls.isInstance(registeredPipeline)) {
      return (T) registeredPipelineMap.get(pipelineName);
    }
    throw new PipeliteException("Unknown pipeline: " + pipelineName);
  }

  /**
   * Returns registered pipelines.
   *
   * @return the registered pipelines.
   */
  public <T extends RegisteredPipeline> List<T> getRegisteredPipelines(Class<T> cls) {
    return registeredPipelineMap.values().stream()
        .filter(s -> cls.isInstance(s))
        .map(s -> (T) s)
        .collect(Collectors.toList());
  }

  @Scheduled(fixedRate = 60, initialDelay = 0, timeUnit = TimeUnit.MINUTES)
  public void updateProcessBacklogCountMetrics() {
    processCountMetricsMap.forEach(
        (k, v) -> v.setBacklogCount(processCountRepository.findProcessBacklogCount(k)));
  }

  @Scheduled(fixedRate = 60, initialDelay = 0, timeUnit = TimeUnit.MINUTES)
  public void updateProcessFailedCountMetrics() {
    processCountMetricsMap.forEach(
        (k, v) -> v.setFailedCount(processCountRepository.findProcessFailedCount(k)));
  }
}
