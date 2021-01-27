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
package pipelite;

import java.util.*;
import java.util.concurrent.ForkJoinPool;
import pipelite.process.Process;
import pipelite.process.builder.ProcessBuilder;
import pipelite.service.RegisteredPipelineServiceTest;

public class TestPipeline implements Pipeline {
  private final String pipelineName =
      UniqueStringGenerator.randomPipelineName(RegisteredPipelineServiceTest.class);
  private final Map<String, Process> processes = new HashMap<>();
  private final int pipelineParallelism;

  public TestPipeline(Collection<Process> processes) {
    addProcesses(processes);
    this.pipelineParallelism = ForkJoinPool.getCommonPoolParallelism();
  }

  @Override
  public String getPipelineName() {
    return pipelineName;
  }

  @Override
  public int getPipelineParallelism() {
    return pipelineParallelism;
  }

  @Override
  public Process createProcess(ProcessBuilder builder) {
    return processes.get(builder.getProcessId());
  }

  public void addProcesses(Collection<Process> processes) {
    processes.stream().forEach(process -> addProcess(process));
  }

  public void addProcess(Process process) {
    this.processes.put(process.getProcessId(), process);
  }
}
