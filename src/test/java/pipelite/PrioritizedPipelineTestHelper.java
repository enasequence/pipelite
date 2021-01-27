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

import com.google.common.util.concurrent.Monitor;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class PrioritizedPipelineTestHelper {

  private final Set<String> newProcesses = ConcurrentHashMap.newKeySet();
  private final Set<String> returnedProcesses = ConcurrentHashMap.newKeySet();
  private final Set<String> acceptedProcesses = ConcurrentHashMap.newKeySet();
  private final Set<String> rejectedProcesses = ConcurrentHashMap.newKeySet();

  public PrioritizedPipelineTestHelper(int processCnt) {
    for (int i = 0; i < processCnt; ++i) {
      newProcesses.add(
          i + "_" + UniqueStringGenerator.randomProcessId(PrioritizedPipelineTestHelper.class));
    }
  }

  private final Monitor monitor = new Monitor();

  public PrioritizedPipeline.NextProcess nextProcess() {
    monitor.enter();
    try {
      if (newProcesses.isEmpty()) {
        return null;
      }
      String processId = newProcesses.iterator().next();
      returnedProcesses.add(processId);
      newProcesses.remove(processId);
      return new PrioritizedPipeline.NextProcess(processId);
    } finally {
      monitor.leave();
    }
  }

  public void confirmProcess(String processId) {
    monitor.enter();
    try {
      acceptedProcesses.add(processId);
    } finally {
      monitor.leave();
    }
  }

  public int getNewProcesses() {
    return newProcesses.size();
  }

  public int getReturnedProcesses() {
    return returnedProcesses.size();
  }

  public int getAcceptedProcesses() {
    return acceptedProcesses.size();
  }

  public int getRejectedProcesses() {
    return rejectedProcesses.size();
  }
}
