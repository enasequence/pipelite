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
package pipelite.launcher;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import pipelite.launcher.process.runner.ProcessRunnerResult;
import pipelite.process.Process;
import pipelite.process.ProcessState;

public class ProcessLauncherStats {

  private final AtomicLong processCreationFailedCount = new AtomicLong(0);
  private final Map<ProcessState, AtomicLong> processExecutionCount = new ConcurrentHashMap<>();
  private final AtomicLong processExceptionCount = new AtomicLong(0);
  private final AtomicLong stageFailedCount = new AtomicLong(0);
  private final AtomicLong stageSuccessCount = new AtomicLong(0);

  public long getProcessCreationFailedCount() {
    return processCreationFailedCount.get();
  }

  public long getProcessExecutionCount(ProcessState state) {
    if (processExecutionCount.get(state) == null) {
      return 0;
    }
    return processExecutionCount.get(state).get();
  }

  public long getProcessExceptionCount() {
    return processExceptionCount.get();
  }

  public long getStageFailedCount() {
    return stageFailedCount.get();
  }

  public long getStageSuccessCount() {
    return stageSuccessCount.get();
  }

  public void addProcessCreationFailedCount(long count) {
    processCreationFailedCount.addAndGet(count);
  }

  private void addProcessExecutionCount(ProcessState state, long count) {
    processExecutionCount.putIfAbsent(state, new AtomicLong(0));
    processExecutionCount.get(state).addAndGet(count);
  }

  public void add(Process process, ProcessRunnerResult result) {
    if (result.getProcessExecutionCount() > 0) {
      addProcessExecutionCount(
          process.getProcessEntity().getState(), result.getProcessExecutionCount());
    }
    processExceptionCount.addAndGet(result.getProcessExceptionCount());
    stageSuccessCount.addAndGet(result.getStageSuccessCount());
    stageFailedCount.addAndGet(result.getStageFailedCount());
  }
}
