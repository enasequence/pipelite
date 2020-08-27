package pipelite.launcher;

import lombok.AllArgsConstructor;

import pipelite.TestInMemoryProcessFactory;
import pipelite.UniqueStringGenerator;
import pipelite.configuration.ProcessConfigurationEx;
import pipelite.executor.TaskExecutor;
import pipelite.executor.TaskExecutorFactory;
import pipelite.instance.ProcessInstance;
import pipelite.instance.ProcessInstanceBuilder;
import pipelite.task.result.TaskExecutionResult;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

@AllArgsConstructor
public class DefaultPipeliteSuccessTaskLauncherTester {

  private final DefaultPipeliteLauncher defaultPipeliteLauncher;
  private final ProcessConfigurationEx processConfiguration;

  private final AtomicInteger processExecutionCount = new AtomicInteger();
  private final Set<String> processExecutionSet = ConcurrentHashMap.newKeySet();
  private final Set<String> processExcessExecutionSet = ConcurrentHashMap.newKeySet();
  private static final int PROCESS_COUNT = 100;
  private static final int TASK_EXECUTION_TIME = 10; // ms

  public class TestTaskExecutionFactory implements TaskExecutorFactory {
    @Override
    public TaskExecutor createTaskExecutor() {
      return taskInstance -> {
        String processId = taskInstance.getProcessId();
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
        return TaskExecutionResult.success();
      };
    }
  }

  private List<ProcessInstance> createProcessInstances() {
    List<ProcessInstance> processInstances = new ArrayList<>();
    for (int i = 0; i < PROCESS_COUNT; ++i) {
      String processName = defaultPipeliteLauncher.getProcessName();
      String processId = "Process" + i;
      processInstances.add(
          new ProcessInstanceBuilder(processName, processId, 9)
              .task(UniqueStringGenerator.randomTaskName(), new TestTaskExecutionFactory())
              .build());
    }
    return processInstances;
  }

  public void test() {

    processConfiguration.setProcessFactory(
        new TestInMemoryProcessFactory(createProcessInstances()));

    defaultPipeliteLauncher.setShutdownPolicy(
        DefaultPipeliteLauncher.ShutdownPolicy.SHUTDOWN_IF_IDLE);
    defaultPipeliteLauncher.setSchedulerDelayMillis(10);

    PipeliteLauncherServiceManager.run(defaultPipeliteLauncher);

    assertThat(processExcessExecutionSet).isEmpty();
    assertThat(processExecutionCount.get()).isEqualTo(PROCESS_COUNT);
    assertThat(processExecutionCount.get())
        .isEqualTo(defaultPipeliteLauncher.getProcessCompletedCount());
    assertThat(defaultPipeliteLauncher.getActiveProcessCount()).isEqualTo(0);
  }
}
