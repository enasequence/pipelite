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
package pipelite.launcher.pipelite;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.AllArgsConstructor;
import pipelite.TestInMemoryProcessFactory;
import pipelite.TestInMemoryProcessSource;
import pipelite.UniqueStringGenerator;
import pipelite.configuration.ProcessConfiguration;
import pipelite.executor.TaskExecutor;
import pipelite.launcher.ServerManager;
import pipelite.process.builder.ProcessBuilder;
import pipelite.process.Process;
import pipelite.task.TaskExecutionResult;

@AllArgsConstructor
public class SuccessPipeliteLauncherTester {

  private final PipeliteLauncher pipeliteLauncher;
  private final ProcessConfiguration processConfiguration;

  private final AtomicInteger processExecutionCount = new AtomicInteger();
  private final Set<String> processExecutionSet = ConcurrentHashMap.newKeySet();
  private final Set<String> processExcessExecutionSet = ConcurrentHashMap.newKeySet();
  private static final int PROCESS_COUNT = 1;
  private static final Duration TASK_EXECUTION_TIME = Duration.ofMillis(10);

  private TaskExecutor createTaskExecutor(String processId) {
    return task -> {
      processExecutionCount.incrementAndGet();
      if (processExecutionSet.contains(processId)) {
        processExcessExecutionSet.add(processId);
      } else {
        processExecutionSet.add(processId);
      }
      try {
        Thread.sleep(TASK_EXECUTION_TIME.toMillis());
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      return TaskExecutionResult.success();
    };
  }

  private List<Process> createProcesses() {
    List<Process> processes = new ArrayList<>();
    for (int i = 0; i < PROCESS_COUNT; ++i) {
      String processName = pipeliteLauncher.getProcessName();
      String processId = "Process" + i;
      processes.add(
          new ProcessBuilder(processName, processId, 9)
              .task(UniqueStringGenerator.randomTaskName())
              .executor(createTaskExecutor(processId))
              .build());
    }
    return processes;
  }

  public void test() {

    List<Process> processes = createProcesses();
    processConfiguration.setProcessFactory(new TestInMemoryProcessFactory(processes));
    processConfiguration.setProcessSource(new TestInMemoryProcessSource(processes));

    pipeliteLauncher.setShutdownIfIdle(true);

    ServerManager.run(pipeliteLauncher, pipeliteLauncher.serviceName());

    assertThat(processExcessExecutionSet).isEmpty();
    assertThat(processExecutionCount.get()).isEqualTo(PROCESS_COUNT);
    assertThat(processExecutionCount.get()).isEqualTo(pipeliteLauncher.getProcessCompletedCount());
    assertThat(pipeliteLauncher.getActiveProcessCount()).isEqualTo(0);
  }
}
