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
import io.micrometer.core.instrument.Timer;
import java.time.Duration;
import java.time.ZonedDateTime;
import pipelite.metrics.helper.MicroMeterHelper;
import pipelite.stage.executor.StageExecutorResult;

public class StageRunnerMetrics extends AbstractMetrics {

  private static final String PREFIX = "pipelite.stage.runner";

  // Micrometer
  private final Timer runOneIterationTimer;

  private final Counter successCounter;
  private final Counter failedCounter;

  public StageRunnerMetrics(String pipelineName, String stageName, MeterRegistry meterRegistry) {
    super(PREFIX);
    String[] tags = MicroMeterHelper.stageTags(pipelineName, stageName);

    runOneIterationTimer = meterRegistry.timer(name("runOneIteration"), tags);

    failedCounter = meterRegistry.counter(name("failed"), tags);
    successCounter = meterRegistry.counter(name("success"), tags);
  }

  public double successCount() {
    return successCounter.count();
  }

  public double failedCount() {
    return failedCounter.count();
  }

  public void endRunOneIteration(ZonedDateTime startTime) {
    runOneIterationTimer.record(Duration.between(startTime, ZonedDateTime.now()));
  }

  public void endStageExecution(StageExecutorResult result) {
    if (result.isSuccess()) {
      successCounter.increment(1);
    } else if (result.isError()) {
      failedCounter.increment(1);
    }
  }
}
