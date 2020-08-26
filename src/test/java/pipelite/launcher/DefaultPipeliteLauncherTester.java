package pipelite.launcher;

import lombok.AllArgsConstructor;

import pipelite.TestInMemoryProcessFactory;
import pipelite.UniqueStringGenerator;
import pipelite.configuration.ProcessConfigurationEx;
import pipelite.configuration.TaskConfiguration;
import pipelite.configuration.TaskConfigurationEx;
import pipelite.instance.ProcessInstance;
import pipelite.instance.TaskInstance;
import pipelite.task.Task;
import pipelite.task.TaskFactory;
import pipelite.task.TaskInfo;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

@AllArgsConstructor
public class DefaultPipeliteLauncherTester {

  private final DefaultPipeliteLauncher defaultPipeliteLauncher;
  private final ProcessConfigurationEx processConfiguration;

  private final AtomicInteger processExecutionCount = new AtomicInteger();
  private final Set<String> processExecutionSet = ConcurrentHashMap.newKeySet();
  private final Set<String> processExcessExecutionSet = ConcurrentHashMap.newKeySet();
  private static final int PROCESS_COUNT = 100;
  private static final int TASK_EXECUTION_TIME = 10; // ms

  public class TestTaskFactory implements TaskFactory {
    @Override
    public Task createTask(TaskInfo taskInfo) {
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
      };
    }
  }

  private List<ProcessInstance> createProcessInstances() {
    TaskFactory taskFactory = new TestTaskFactory();
    TaskConfigurationEx taskConfiguration = new TaskConfigurationEx(new TaskConfiguration());

    List<ProcessInstance> processInstances = new ArrayList<>();
    for (int i = 0; i < PROCESS_COUNT; ++i) {
      String processName = defaultPipeliteLauncher.getProcessName();
      String processId = "Process" + i;

      TaskInstance taskInstance1 =
          TaskInstance.builder()
              .processName(processName)
              .processId(processId)
              .taskName(UniqueStringGenerator.randomTaskName())
              .taskFactory(taskFactory)
              .taskParameters(taskConfiguration)
              .build();

      processInstances.add(
          ProcessInstance.builder()
              .processName(processName)
              .processId(processId)
              .priority(9)
              .tasks(Arrays.asList(taskInstance1))
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
