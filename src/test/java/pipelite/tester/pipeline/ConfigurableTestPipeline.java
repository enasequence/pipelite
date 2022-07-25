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
package pipelite.tester.pipeline;

import com.google.common.util.concurrent.Monitor;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import pipelite.Pipeline;
import pipelite.PipeliteIdCreator;
import pipelite.tester.process.TestProcessConfiguration;

/**
 * Creates processes for testing purposes. The process configuration is defined by
 * TestProcessConfiguration.
 */
public class ConfigurableTestPipeline<T extends TestProcessConfiguration>
    extends ConfigurableTestRegisteredPipeline<T> implements Pipeline {

  private final int parallelism;

  private final Set<String> createdProcessIds = ConcurrentHashMap.newKeySet();
  private final Set<String> returnedProcessIds = ConcurrentHashMap.newKeySet();
  private final Set<String> confirmedProcessIds = ConcurrentHashMap.newKeySet();
  private final Monitor monitor = new Monitor();

  public ConfigurableTestPipeline(int parallelism, int processCount, T testProcessConfiguration) {
    super(testProcessConfiguration);
    this.parallelism = parallelism;
    for (int i = 0; i < processCount; ++i) {
      String processId = PipeliteIdCreator.processId();
      createdProcessIds.add(processId);
    }
  }

  public int parallelism() {
    return parallelism;
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
}
