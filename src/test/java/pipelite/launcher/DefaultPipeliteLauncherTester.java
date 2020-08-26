package pipelite.launcher;

import com.google.common.util.concurrent.Monitor;
import lombok.AllArgsConstructor;

import pipelite.UniqueStringGenerator;
import pipelite.configuration.ProcessConfigurationEx;
import pipelite.configuration.TaskConfiguration;
import pipelite.configuration.TaskConfigurationEx;
import pipelite.instance.ProcessInstance;
import pipelite.instance.ProcessInstanceFactory;
import pipelite.instance.TaskInstance;
import pipelite.stage.Stage;
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

  private class TestProcessFactory implements ProcessInstanceFactory {

    private final Set<ProcessInstance> newProcessInstances = new HashSet<>();
    private final Map<String, ProcessInstance> receivedProcessInstances = new HashMap<>();
    private final Map<String, ProcessInstance> confirmedProcessInstances = new HashMap<>();

    private final TaskFactory taskFactory = new TestTaskFactory();
    private final TaskConfigurationEx taskConfiguration =
        new TaskConfigurationEx(new TaskConfiguration());

    private final Monitor monitor = new Monitor();

    public TestProcessFactory() {
      for (int i = 0; i < PROCESS_COUNT; ++i) {
        createProcessInstance(i);
      }
    }

    private void createProcessInstance(int i) {
      String processName = defaultPipeliteLauncher.getProcessName();
      String processId = "Process" + i;

      Stage stage1 =
          Stage.builder()
              .stageName(UniqueStringGenerator.randomStageName())
              .taskFactory(taskFactory)
              .build();

      TaskInstance taskInstance1 =
          TaskInstance.builder()
              .processName(processName)
              .processId(processId)
              .stage(stage1)
              .taskParameters(taskConfiguration)
              .build();

      newProcessInstances.add(
          ProcessInstance.builder()
              .processName(processName)
              .processId(processId)
              .priority(9)
              .tasks(Arrays.asList(taskInstance1))
              .build());
    }

    @Override
    public ProcessInstance receive() {
      monitor.enter();
      try {
        if (newProcessInstances.isEmpty()) {
          return null;
        }
        ProcessInstance processInstance = newProcessInstances.iterator().next();
        receivedProcessInstances.put(processInstance.getProcessId(), processInstance);
        newProcessInstances.remove(processInstance);
        return processInstance;
      } finally {
        monitor.leave();
      }
    }

    @Override
    public void confirm(ProcessInstance processInstance) {
      monitor.enter();
      try {
        confirmedProcessInstances.put(
            processInstance.getProcessId(),
            receivedProcessInstances.remove(processInstance.getProcessId()));
      } finally {
        monitor.leave();
      }
    }

    @Override
    public void reject(ProcessInstance processInstance) {
      monitor.enter();
      try {
        newProcessInstances.add(receivedProcessInstances.remove(processInstance.getProcessId()));
      } finally {
        monitor.leave();
      }
    }

    @Override
    public ProcessInstance load(String processId) {
      monitor.enter();
      try {
        return confirmedProcessInstances.get(processId);
      } finally {
        monitor.leave();
      }
    }
  }

  public void test() {

    processConfiguration.setProcessFactory(new TestProcessFactory());

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
