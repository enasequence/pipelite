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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import lombok.extern.flogger.Flogger;
import pipelite.exception.PipeliteException;
import pipelite.metrics.helper.MicroMeterHelper;
import pipelite.process.ProcessState;

@Flogger
public class ProcessRunnerMetrics extends AbstractMetrics {

  private static final String PREFIX = "pipelite.process";

  private final MeterRegistry meterRegistry;
  private final String pipelineName;
  private final Map<String, StageRunnerMetrics> stageRunnerMetrics = new ConcurrentHashMap<>();

  // Micrometer metrics
  private final AtomicDouble runningGauge = new AtomicDouble();

  private final Counter completedCounter;
  private final Counter failedCounter;

  public ProcessRunnerMetrics(String pipelineName, MeterRegistry meterRegistry) {
    super(PREFIX);
    this.pipelineName = pipelineName;
    this.meterRegistry = meterRegistry;
    String[] runningTags = MicroMeterHelper.pipelineTags(pipelineName);
    String[] completedTags = MicroMeterHelper.pipelineTags(pipelineName, "COMPLETED");
    String[] failedTags = MicroMeterHelper.pipelineTags(pipelineName, "FAILED");
    Gauge.builder(name("running"), () -> runningGauge.get())
        .tags(runningTags)
        .register(meterRegistry);
    completedCounter = meterRegistry.counter(name("status"), completedTags);
    failedCounter = meterRegistry.counter(name("status"), failedTags);
  }

  public StageRunnerMetrics stage(String stageName) {
    if (stageName == null) {
      throw new PipeliteException("Missing stage name");
    }
    StageRunnerMetrics m = stageRunnerMetrics.get(stageName);
    if (m != null) {
      return m;
    }
    return stageRunnerMetrics.computeIfAbsent(
        stageName, k -> new StageRunnerMetrics(pipelineName, stageName, meterRegistry));
  }

  public double completedCount() {
    return completedCounter.count();
  }

  public double failedCount() {
    return failedCounter.count();
  }

  public double stageSuccessCount() {
    return stageRunnerMetrics.values().stream()
        .mapToDouble(m -> m.successCount())
        .reduce(0, Double::sum);
  }

  public double stageFailedCount() {
    return stageRunnerMetrics.values().stream()
        .mapToDouble(m -> m.failedCount())
        .reduce(0, Double::sum);
  }

  /**
   * Set the number of running processes.
   *
   * @param count the number of running processes
   */
  public void setRunningProcessesCount(int count) {
    log.atFiner().log("Running processes count for " + pipelineName + ": " + count);
    runningGauge.set(count);
  }

  public void endProcessExecution(ProcessState state) {
    if (state == ProcessState.COMPLETED) {
      completedCounter.increment(1);
    } else if (state == ProcessState.FAILED) {
      failedCounter.increment(1);
    }
  }
}
