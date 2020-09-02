package pipelite.launcher;

import lombok.AllArgsConstructor;

import pipelite.TestInMemoryProcessFactory;
import pipelite.TestInMemoryProcessSource;
import pipelite.UniqueStringGenerator;
import pipelite.configuration.ProcessConfiguration;
import pipelite.executor.TaskExecutor;
import pipelite.process.ProcessInstance;
import pipelite.process.ProcessBuilder;
import pipelite.resolver.ResultResolver;
import pipelite.task.TaskExecutionResult;

import java.time.Duration;
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
  private static final int PROCESS_COUNT = 10;
  private static final Duration SCHEDULER_DELAY = Duration.ofMillis(250);
  private static final Duration TASK_EXECUTION_TIME = Duration.ofMillis(10);

  private TaskExecutor createTaskExecutor(String processId) {
    return taskInstance -> {
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

  private List<ProcessInstance> createProcessInstances() {
    List<ProcessInstance> processInstances = new ArrayList<>();
    for (int i = 0; i < PROCESS_COUNT; ++i) {
      String processName = pipeliteLauncher.getProcessName();
      String processId = "Process" + i;
      processInstances.add(
          new ProcessBuilder(processName, processId, 9)
              .task(
                  UniqueStringGenerator.randomTaskName(),
                  createTaskExecutor(processId),
                  ResultResolver.DEFAULT_EXCEPTION_RESOLVER)
              .build());
    }
    return processInstances;
  }

  public void test() {

    List<ProcessInstance> processInstances = createProcessInstances();
    processConfiguration.setProcessFactory(new TestInMemoryProcessFactory(processInstances));
    processConfiguration.setProcessSource(new TestInMemoryProcessSource(processInstances));

    pipeliteLauncher.setShutdownPolicy(ShutdownPolicy.SHUTDOWN_IF_IDLE);
    pipeliteLauncher.setSchedulerDelay(SCHEDULER_DELAY);

    ServerManager.run(pipeliteLauncher, pipeliteLauncher.serviceName());

    assertThat(processExcessExecutionSet).isEmpty();
    assertThat(processExecutionCount.get()).isEqualTo(PROCESS_COUNT);
    assertThat(processExecutionCount.get()).isEqualTo(pipeliteLauncher.getProcessCompletedCount());
    assertThat(pipeliteLauncher.getActiveProcessCount()).isEqualTo(0);
  }
}
