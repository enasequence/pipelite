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
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import pipelite.metrics.helper.MicroMeterHelper;
import pipelite.stage.executor.StageExecutorResult;

public class StageRunnerMetrics extends AbstractMetrics {

  private static final String PREFIX = "pipelite.stage";

  // Micrometer metrics
  private final AtomicDouble runningGauge = new AtomicDouble();
  private final Counter successCounter;
  private final Counter failedCounter;

  public StageRunnerMetrics(String pipelineName, String stageName, MeterRegistry meterRegistry) {
    super(PREFIX);
    String[] runningTags = MicroMeterHelper.stageTags(pipelineName, stageName);
    String[] successTags = MicroMeterHelper.stageTags(pipelineName, stageName, "SUCCESS");
    String[] failedTags = MicroMeterHelper.stageTags(pipelineName, stageName, "FAILED");

    Gauge.builder(name("running"), () -> runningGauge.get())
        .tags(runningTags)
        .register(meterRegistry);
    successCounter = meterRegistry.counter(name("status"), successTags);
    failedCounter = meterRegistry.counter(name("status"), failedTags);
  }

  /**
   * Set the number of running stages.
   *
   * @param count the number of running stages
   */
  public void setRunningStagesCount(int count) {
    runningGauge.set(count);
  }

  public double successCount() {
    return successCounter.count();
  }

  public double failedCount() {
    return failedCounter.count();
  }

  public void endStageExecution(StageExecutorResult result) {
    if (result.isSuccess()) {
      successCounter.increment(1);
    } else if (result.isError()) {
      failedCounter.increment(1);
    }
  }
}
