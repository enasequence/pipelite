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

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.time.Duration;
import java.time.ZonedDateTime;
import pipelite.metrics.helper.MicroMeterHelper;

public class StageExecutorMetrics extends AbstractMetrics {

  private static final String PREFIX = "pipelite.stage.executor";

  // Micrometer
  private final Timer submitTimer;

  public StageExecutorMetrics(String pipelineName, String stageName, MeterRegistry meterRegistry) {
    super(PREFIX);
    String[] tags = MicroMeterHelper.stageTags(pipelineName, stageName);

    submitTimer = meterRegistry.timer(name("submit"), tags);
  }

  public void endSubmit(ZonedDateTime startTime) {
    submitTimer.record(Duration.between(startTime, ZonedDateTime.now()));
  }
}
