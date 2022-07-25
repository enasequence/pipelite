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
import pipelite.metrics.collector.StageExecutorMetrics;
import pipelite.metrics.collector.StageRunnerMetrics;

public class StageMetrics {

  // Micrometer
  private final StageRunnerMetrics stageRunnerMetrics;
  private final StageExecutorMetrics stageExecutorMetrics;

  public StageMetrics(String pipelineName, String stageName, MeterRegistry meterRegistry) {
    stageRunnerMetrics = new StageRunnerMetrics(pipelineName, stageName, meterRegistry);
    stageExecutorMetrics = new StageExecutorMetrics(pipelineName, stageName, meterRegistry);
  }

  public StageRunnerMetrics runner() {
    return stageRunnerMetrics;
  }

  public StageExecutorMetrics executor() {
    return stageExecutorMetrics;
  }
}
