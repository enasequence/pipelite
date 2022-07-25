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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import pipelite.exception.PipeliteException;
import pipelite.metrics.collector.ProcessRunnerMetrics;

public class ProcessMetrics {

  private final MeterRegistry meterRegistry;
  private final String pipelineName;

  // Micrometer
  private final ProcessRunnerMetrics processRunnerMetrics;
  private final Map<String, StageMetrics> stageMetrics = new ConcurrentHashMap<>();

  public ProcessMetrics(String pipelineName, MeterRegistry meterRegistry) {
    this.meterRegistry = meterRegistry;
    this.pipelineName = pipelineName;
    this.processRunnerMetrics = new ProcessRunnerMetrics(pipelineName, meterRegistry);
  }

  public ProcessRunnerMetrics runner() {
    return processRunnerMetrics;
  }

  public StageMetrics stage(String stageName) {
    if (stageName == null) {
      throw new PipeliteException("Missing stage name");
    }
    try {
      StageMetrics m = stageMetrics.get(stageName);
      if (m == null) {
        stageMetrics.putIfAbsent(
            stageName, new StageMetrics(pipelineName, stageName, meterRegistry));
        return stageMetrics.get(stageName);
      }
      return m;
    } catch (Exception ex) {
      stageMetrics.putIfAbsent(stageName, new StageMetrics(pipelineName, stageName, meterRegistry));
      return stageMetrics.get(stageName);
    }
  }

  public double stageSuccessCount() {
    return stageMetrics.values().stream()
        .mapToDouble(m -> m.runner().successCount())
        .reduce(0, Double::sum);
  }

  public double stageFailedCount() {
    return stageMetrics.values().stream()
        .mapToDouble(m -> m.runner().failedCount())
        .reduce(0, Double::sum);
  }
}
