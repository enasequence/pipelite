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
package pipelite.launcher.process.runner;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Duration;
import pipelite.process.ProcessState;

public class ProcessRunnerMetrics {

  private static final Duration MAX_WINDOW = Duration.ofHours(24);

  private final Counter processCompletedCounter;
  private final Counter processFailedCounter;
  private final Counter stageSuccessCounter;
  private final Counter stageFailedCounter;
  private final Counter internalErrorCounter;

  private final ProcessRunnerCounter processCompletedCustomCounter = new ProcessRunnerCounter();
  private final ProcessRunnerCounter processFailedCustomCounter = new ProcessRunnerCounter();
  private final ProcessRunnerCounter stageSuccessCustomCounter = new ProcessRunnerCounter();
  private final ProcessRunnerCounter stageFailedCustomCounter = new ProcessRunnerCounter();
  private final ProcessRunnerCounter internalErrorCustomCounter = new ProcessRunnerCounter();

  public ProcessRunnerMetrics(String pipelineName, MeterRegistry meterRegistry) {
    processCompletedCounter =
        meterRegistry.counter("pipelite.process.completed", "pipelineName", pipelineName);
    processFailedCounter =
        meterRegistry.counter("pipelite.process.failed", "pipelineName", pipelineName);
    stageFailedCounter =
        meterRegistry.counter("pipelite.stage.failed", "pipelineName", pipelineName);
    stageSuccessCounter =
        meterRegistry.counter("pipelite.stage.success", "pipelineName", pipelineName);
    internalErrorCounter =
        meterRegistry.counter("pipelite.stage.error", "pipelineName", pipelineName);
  }

  public double getProcessCompletedCount() {
    return processCompletedCounter.count();
  }

  public double getProcessFailedCount() {
    return processFailedCounter.count();
  }

  public double getStageSuccessCount() {
    return stageSuccessCounter.count();
  }

  public double getStageFailedCount() {
    return stageFailedCounter.count();
  }

  public double getInternalErrorCount() {
    return internalErrorCounter.count();
  }

  public double getProcessCompletedCount(Duration since) {
    return processCompletedCustomCounter.getCount(since);
  }

  public double getProcessFailedCount(Duration since) {
    return processFailedCustomCounter.getCount(since);
  }

  public double getStageSuccessCount(Duration since) {
    return stageSuccessCustomCounter.getCount(since);
  }

  public double getStageFailedCount(Duration since) {
    return stageFailedCustomCounter.getCount(since);
  }

  public double getInternalErrorCount(Duration since) {
    return internalErrorCustomCounter.getCount(since);
  }

  public void internalError() {
    internalErrorCounter.increment(1);
    internalErrorCustomCounter.increment(1);
  }

  public void processRunnerResult(ProcessState state, ProcessRunnerResult result) {
    if (state == ProcessState.COMPLETED) {
      processCompletedCounter.increment(1);
      processCompletedCustomCounter.increment(1);
    }
    if (state == ProcessState.FAILED) {
      processFailedCounter.increment(1);
      processFailedCustomCounter.increment(1);
    }
    stageSuccessCounter.increment(result.getStageSuccessCount());
    stageSuccessCustomCounter.increment(result.getStageSuccessCount());
    stageFailedCounter.increment(result.getStageFailedCount());
    stageFailedCustomCounter.increment(result.getStageFailedCount());
    internalErrorCounter.increment(result.getInternalErrorCount());
    internalErrorCustomCounter.increment(result.getInternalErrorCount());
  }

  public void purgeCustomCounters() {
    purgeCustomCounters(MAX_WINDOW);
  }

  public void purgeCustomCounters(Duration since) {
    processCompletedCustomCounter.purge(since);
    processFailedCustomCounter.purge(since);
    stageSuccessCustomCounter.purge(since);
    stageFailedCustomCounter.purge(since);
    internalErrorCustomCounter.purge(since);
  }
}
