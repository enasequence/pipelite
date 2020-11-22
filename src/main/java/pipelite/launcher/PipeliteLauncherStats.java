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
import pipelite.process.ProcessState;

public class PipeliteLauncherStats {

  final AtomicLong processIdMissingCount = new AtomicLong(0);
  final AtomicLong processIdNotUniqueCount = new AtomicLong(0);
  final AtomicLong processCreationFailedCount = new AtomicLong(0);
  private final Map<ProcessState, AtomicLong> processExecutionCount = new ConcurrentHashMap<>();
  final AtomicLong processExceptionCount = new AtomicLong(0);
  final AtomicLong stageFailedCount = new AtomicLong(0);
  final AtomicLong stageSuccessCount = new AtomicLong(0);

  public long getProcessIdMissingCount() {
    return processIdMissingCount.get();
  }

  public long getProcessIdNotUniqueCount() {
    return processIdNotUniqueCount.get();
  }

  public long getProcessCreationFailedCount() {
    return processCreationFailedCount.get();
  }

  AtomicLong setProcessExecutionCount(ProcessState state) {
    processExecutionCount.putIfAbsent(state, new AtomicLong(0));
    return processExecutionCount.get(state);
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
}
