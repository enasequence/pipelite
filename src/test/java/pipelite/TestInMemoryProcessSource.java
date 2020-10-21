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
import pipelite.process.Process;
import pipelite.process.ProcessSource;

public class TestInMemoryProcessSource implements ProcessSource {

  private final String pipelineName;
  private final Set<Process> newProcesses = new HashSet<>();
  private final Map<String, Process> returnedProcesses = new HashMap<>();
  private final Map<String, Process> acceptedProcesses = new HashMap<>();
  private final Set<Process> rejectedProcesses = new HashSet<>();

  public TestInMemoryProcessSource(String pipelineName, Collection<Process> processes) {
    this.pipelineName = pipelineName;
    newProcesses.addAll(processes);
  }

  @Override
  public String getPipelineName() {
    return pipelineName;
  }

  private boolean permanentRejection = false;

  private final Monitor monitor = new Monitor();

  @Override
  public NewProcess next() {
    monitor.enter();
    try {
      if (newProcesses.isEmpty()) {
        return null;
      }
      Process process = newProcesses.iterator().next();
      returnedProcesses.put(process.getProcessId(), process);
      newProcesses.remove(process);
      return new NewProcess(process.getProcessId(), 9);
    } finally {
      monitor.leave();
    }
  }

  @Override
  public void accept(String processId) {
    monitor.enter();
    try {
      acceptedProcesses.put(processId, returnedProcesses.remove(processId));
    } finally {
      monitor.leave();
    }
  }

  @Override
  public void reject(String processId) {
    monitor.enter();
    try {
      if (permanentRejection) {
        rejectedProcesses.add(returnedProcesses.remove(processId));
      } else {
        newProcesses.add(returnedProcesses.remove(processId));
      }
    } finally {
      monitor.leave();
    }
  }

  public boolean isPermanentRejection() {
    return permanentRejection;
  }

  public void setPermanentRejection(boolean permanentRejection) {
    this.permanentRejection = permanentRejection;
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
