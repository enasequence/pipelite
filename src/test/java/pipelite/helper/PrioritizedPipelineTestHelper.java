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
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import pipelite.PrioritizedPipeline;
import pipelite.UniqueStringGenerator;

public abstract class PrioritizedPipelineTestHelper extends PipelineTestHelper
    implements PrioritizedPipeline {

  private final int processCnt;
  private final Set<String> newProcessIds = ConcurrentHashMap.newKeySet();
  private final Set<String> returnedProcessIds = ConcurrentHashMap.newKeySet();
  private final Set<String> confirmedProcessIds = ConcurrentHashMap.newKeySet();
  private final Monitor monitor = new Monitor();

  public PrioritizedPipelineTestHelper(int parallelism, int processCnt) {
    this(
        UniqueStringGenerator.randomPipelineName(PrioritizedPipelineTestHelper.class),
        parallelism,
        processCnt);
  }

  public PrioritizedPipelineTestHelper(String pipelineName, int parallelism, int processCnt) {
    super(pipelineName, parallelism);
    this.processCnt = processCnt;
    for (int i = 0; i < processCnt; ++i) {
      newProcessIds.add(UniqueStringGenerator.randomProcessId(PrioritizedPipelineTestHelper.class));
    }
  }

  public int processCnt() {
    return processCnt;
  }

  public PrioritizedPipeline.PrioritizedProcess nextProcess() {
    monitor.enter();
    try {
      if (newProcessIds.isEmpty()) {
        return null;
      }
      String processId = newProcessIds.iterator().next();
      returnedProcessIds.add(processId);
      newProcessIds.remove(processId);
      return new PrioritizedPipeline.PrioritizedProcess(processId);
    } finally {
      monitor.leave();
    }
  }

  public void confirmProcess(String processId) {
    monitor.enter();
    try {
      confirmedProcessIds.add(processId);
    } finally {
      monitor.leave();
    }
  }

  public int getNewProcessCount() {
    return newProcessIds.size();
  }

  public int getReturnedProcessCount() {
    return returnedProcessIds.size();
  }

  public int getConfirmedProcessCount() {
    return confirmedProcessIds.size();
  }

  public Collection<String> getNewProcessIds() {
    return newProcessIds;
  }

  public Collection<String> getReturnedProcessIds() {
    return returnedProcessIds;
  }

  public Collection<String> getConfirmedProcessIds() {
    return confirmedProcessIds;
  }
}
