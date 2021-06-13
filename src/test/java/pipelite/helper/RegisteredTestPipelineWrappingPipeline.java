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
package pipelite.helper;

import com.google.common.util.concurrent.Monitor;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import pipelite.Pipeline;
import pipelite.RegisteredPipeline;
import pipelite.UniqueStringGenerator;
import pipelite.process.builder.ProcessBuilder;

public class RegisteredTestPipelineWrappingPipeline<T extends RegisteredPipeline>
    implements Pipeline {

  private final int parallelism;
  private final int processCount;
  private final T registeredTestPipeline;
  private final Set<String> createdProcessIds = ConcurrentHashMap.newKeySet();
  private final Set<String> returnedProcessIds = ConcurrentHashMap.newKeySet();
  private final Set<String> confirmedProcessIds = ConcurrentHashMap.newKeySet();
  private final Monitor monitor = new Monitor();

  public RegisteredTestPipelineWrappingPipeline(
      int parallelism, int processCount, T registeredTestPipeline) {
    this.parallelism = parallelism;
    this.processCount = processCount;
    this.registeredTestPipeline = registeredTestPipeline;
    for (int i = 0; i < processCount; ++i) {
      createdProcessIds.add(
          UniqueStringGenerator.randomProcessId(RegisteredTestPipelineWrappingPipeline.class));
    }
  }

  public int parallelism() {
    return parallelism;
  }

  public int processCnt() {
    return processCount;
  }

  public T getRegisteredTestPipeline() {
    return registeredTestPipeline;
  }

  @Override
  public final String pipelineName() {
    return registeredTestPipeline.pipelineName();
  }

  @Override
  public final void configureProcess(ProcessBuilder builder) {
    registeredTestPipeline.configureProcess(builder);
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

  /** Creates a process for without using PipelineRunner or ScheduleRunner. */
  public pipelite.process.Process createProcess() {
    String processId =
        UniqueStringGenerator.randomProcessId(RegisteredSingleStageTestPipeline.class);
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
