package pipelite.launcher;

import lombok.AllArgsConstructor;

import pipelite.TestInMemoryProcessFactory;
import pipelite.UniqueStringGenerator;
import pipelite.configuration.ProcessConfiguration;
import pipelite.executor.TaskExecutor;
import pipelite.instance.ProcessInstance;
import pipelite.instance.ProcessInstanceBuilder;
import pipelite.resolver.ResultResolver;
import pipelite.task.TaskExecutionResult;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

@AllArgsConstructor
public class SuccessPipeliteLauncherTester {

  private final PipeliteLauncher pipeliteLauncher;
  private final ProcessConfiguration processConfiguration;

  private final AtomicInteger processExecutionCount = new AtomicInteger();
  private final Set<String> processExecutionSet = ConcurrentHashMap.newKeySet();
  private final Set<String> processExcessExecutionSet = ConcurrentHashMap.newKeySet();
  private static final int PROCESS_COUNT = 100;
  private static final int TASK_EXECUTION_TIME = 10; // ms
  private static final int SCHEDULER_DELAY = 100; // ms

  private TaskExecutor createTaskExecutor(String processId) {
    return taskInstance -> {
      processExecutionCount.incrementAndGet();
      if (processExecutionSet.contains(processId)) {
        processExcessExecutionSet.add(processId);
      } else {
        processExecutionSet.add(processId);
      }
      try {
        Thread.sleep(TASK_EXECUTION_TIME);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      return TaskExecutionResult.defaultSuccess();
    };
  }

  private List<ProcessInstance> createProcessInstances() {
    List<ProcessInstance> processInstances = new ArrayList<>();
    for (int i = 0; i < PROCESS_COUNT; ++i) {
      String processName = pipeliteLauncher.getProcessName();
      String processId = "Process" + i;
      processInstances.add(
          new ProcessInstanceBuilder(processName, processId, 9)
              .task(
                  UniqueStringGenerator.randomTaskName(),
                  createTaskExecutor(processId),
                  ResultResolver.DEFAULT_EXCEPTION_RESOLVER)
              .build());
    }
    return processInstances;
  }

  public void test() {

    processConfiguration.setProcessFactory(
        new TestInMemoryProcessFactory(createProcessInstances()));

    pipeliteLauncher.setShutdownPolicy(PipeliteLauncher.ShutdownPolicy.SHUTDOWN_IF_IDLE);
    pipeliteLauncher.setSchedulerDelayMillis(SCHEDULER_DELAY);

    PipeliteLauncherServiceManager.run(pipeliteLauncher);

    assertThat(processExcessExecutionSet).isEmpty();
    assertThat(processExecutionCount.get()).isEqualTo(PROCESS_COUNT);
    assertThat(processExecutionCount.get()).isEqualTo(pipeliteLauncher.getProcessCompletedCount());
    assertThat(pipeliteLauncher.getActiveProcessCount()).isEqualTo(0);
  }
}
