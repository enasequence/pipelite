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

import com.google.common.util.concurrent.Monitor;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import pipelite.Pipeline;
import pipelite.RegisteredPipeline;
import pipelite.UniqueStringGenerator;
import pipelite.process.builder.ProcessBuilder;
import pipelite.tester.process.SingleStageTestProcessConfiguration;

/**
 * The configurable test pipeline creates processes for testing purposes. The created processes are
 * configured using the given RegisteredPipeline. The process execution parallelism and the number
 * of processes are defined. The configurable test pipeline should to be registered in tests for the
 * processes to be created and executed. Alternatively, processes can be created by calling
 * createProcess and executed explicitly.
 */
public class ConfigurableTestPipeline<T extends RegisteredPipeline> implements Pipeline {

  private final int parallelism;
  private final int processCount;
  private final T registeredPipeline;

  private final Set<String> createdProcessIds = ConcurrentHashMap.newKeySet();
  private final Set<String> returnedProcessIds = ConcurrentHashMap.newKeySet();
  private final Set<String> confirmedProcessIds = ConcurrentHashMap.newKeySet();
  private final Monitor monitor = new Monitor();

  public ConfigurableTestPipeline(int parallelism, int processCount, T registeredPipeline) {
    this.parallelism = parallelism;
    this.processCount = processCount;
    this.registeredPipeline = registeredPipeline;
    for (int i = 0; i < processCount; ++i) {
      createdProcessIds.add(UniqueStringGenerator.randomProcessId(ConfigurableTestPipeline.class));
    }
  }

  public int parallelism() {
    return parallelism;
  }

  public int processCnt() {
    return processCount;
  }

  public T getRegisteredPipeline() {
    return registeredPipeline;
  }

  @Override
  public final String pipelineName() {
    return registeredPipeline.pipelineName();
  }

  @Override
  public final void configureProcess(ProcessBuilder builder) {
    registeredPipeline.configureProcess(builder);
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

  /**
   * Creates a process without registering the pipeline. The created processes should be executed
   * explicitly.
   */
  public pipelite.process.Process createProcess() {
    String processId =
        UniqueStringGenerator.randomProcessId(SingleStageTestProcessConfiguration.class);
    ProcessBuilder processBuilder = new ProcessBuilder(processId);
    configureProcess(processBuilder);
    return processBuilder.build();
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
}
