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
package pipelite.tester.pipeline;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.util.concurrent.Monitor;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import pipelite.Pipeline;
import pipelite.UniqueStringGenerator;
import pipelite.metrics.PipeliteMetrics;
import pipelite.process.builder.ProcessBuilder;
import pipelite.service.ProcessService;
import pipelite.service.StageService;
import pipelite.tester.process.SingleStageTestProcessConfiguration;
import pipelite.tester.process.TestProcessConfiguration;

/**
 * The configurable test pipeline creates processes for testing purposes. The created processes are
 * configured using the given RegisteredPipeline. The process execution parallelism and the number
 * of processes are defined. The configurable test pipeline should to be registered in tests for the
 * processes to be created and executed. Alternatively, processes can be created by calling
 * createProcess and executed explicitly.
 */
public class ConfigurableTestPipeline<T extends TestProcessConfiguration> implements Pipeline {

  private final int parallelism;
  private final int processCount;
  private final T testProcessConfiguration;

  private final Set<String> createdProcessIds = ConcurrentHashMap.newKeySet();
  private final Set<String> returnedProcessIds = ConcurrentHashMap.newKeySet();
  private final Set<String> confirmedProcessIds = ConcurrentHashMap.newKeySet();
  private final Monitor monitor = new Monitor();

  public ConfigurableTestPipeline(int parallelism, int processCount, T testProcessConfiguration) {
    this.parallelism = parallelism;
    this.processCount = processCount;
    this.testProcessConfiguration = testProcessConfiguration;
    for (int i = 0; i < processCount; ++i) {
      String processId = UniqueStringGenerator.randomProcessId(ConfigurableTestPipeline.class);
      createdProcessIds.add(processId);
    }
  }

  public int parallelism() {
    return parallelism;
  }

  public int processCnt() {
    return processCount;
  }

  public T testProcessConfiguration() {
    return testProcessConfiguration;
  }

  @Override
  public final String pipelineName() {
    return testProcessConfiguration.pipelineName();
  }

  @Override
  public final void configureProcess(ProcessBuilder builder) {
    testProcessConfiguration.configureProcess(builder);
  }

  @Override
  public final Options configurePipeline() {
    return new Options().pipelineParallelism(parallelism);
  }

  public final Process nextProcess() {
    monitor.enter();
    try {
      if (createdProcessIds.isEmpty()) {
        return null;
      }
      String processId = createdProcessIds.iterator().next();
      returnedProcessIds.add(processId);
      createdProcessIds.remove(processId);
      return new Process(processId);
    } finally {
      monitor.leave();
    }
  }

  public final void confirmProcess(String processId) {
    monitor.enter();
    try {
      confirmedProcessIds.add(processId);
    } finally {
      monitor.leave();
    }
  }

  public int createdProcessCount() {
    return createdProcessIds.size();
  }

  public int returnedProcessCount() {
    return returnedProcessIds.size();
  }

  public int confirmedProcessCount() {
    return confirmedProcessIds.size();
  }

  public void assertCompleted(
      ProcessService processService, StageService stageService, PipeliteMetrics metrics) {
    assertThat(testProcessConfiguration.configuredProcessIds().size()).isEqualTo(processCount);
    if (testProcessConfiguration instanceof SingleStageTestProcessConfiguration) {
      ((SingleStageTestProcessConfiguration) testProcessConfiguration)
          .assertCompleted(processService, stageService, metrics, processCount);
    }
  }
}
