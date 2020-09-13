package pipelite.launcher.process;

import org.junit.jupiter.api.Test;
import pipelite.UniqueStringGenerator;
import pipelite.entity.PipeliteStage;
import pipelite.executor.SuccessTaskExecutor;
import pipelite.process.ProcessInstance;
import pipelite.process.builder.ProcessBuilder;
import pipelite.task.TaskExecutionResult;
import pipelite.task.TaskInstance;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class DependencyResolverTest {

  @Test
  public void testGetRunnableTaskAllActiveAllDependOnPrevious() {
    List<ProcessLauncher.PipeliteTaskInstance> pipeliteTaskInstances = new ArrayList();

    ProcessBuilder builder =
        new ProcessBuilder(
            UniqueStringGenerator.randomProcessName(), UniqueStringGenerator.randomProcessId());
    ProcessInstance processInstance =
        builder
            .task("TASK1")
            .executor(new SuccessTaskExecutor())
            .taskDependsOnPrevious("TASK2")
            .executor(new SuccessTaskExecutor())
            .taskDependsOnPrevious("TASK3")
            .executor(new SuccessTaskExecutor())
            .build();

    for (TaskInstance taskInstance : processInstance.getTasks()) {
      PipeliteStage pipeliteStage = new PipeliteStage();
      pipeliteStage.startExecution(taskInstance);
      pipeliteTaskInstances.add(
          new ProcessLauncher.PipeliteTaskInstance(taskInstance, pipeliteStage));
    }

    DependencyResolver dependencyResolver = new DependencyResolver(pipeliteTaskInstances);

    List<ProcessLauncher.PipeliteTaskInstance> runnableTasks =
        dependencyResolver.getRunnableTasks();

    assertThat(runnableTasks.size()).isOne();
    assertThat(runnableTasks.get(0).getTaskInstance().getTaskName()).isEqualTo("TASK1");
  }

  @Test
  public void testGetRunnableTaskAllActiveAllDependOnFirst() {
    List<ProcessLauncher.PipeliteTaskInstance> pipeliteTaskInstances = new ArrayList();

    ProcessBuilder builder =
        new ProcessBuilder(
            UniqueStringGenerator.randomProcessName(), UniqueStringGenerator.randomProcessId());
    ProcessInstance processInstance =
        builder
            .task("TASK1")
            .executor(new SuccessTaskExecutor())
            .taskDependsOn("TASK2", "TASK1")
            .executor(new SuccessTaskExecutor())
            .taskDependsOn("TASK3", "TASK1")
            .executor(new SuccessTaskExecutor())
            .build();

    for (TaskInstance taskInstance : processInstance.getTasks()) {
      PipeliteStage pipeliteStage = new PipeliteStage();
      pipeliteStage.startExecution(taskInstance);
      pipeliteTaskInstances.add(
          new ProcessLauncher.PipeliteTaskInstance(taskInstance, pipeliteStage));
    }

    DependencyResolver dependencyResolver = new DependencyResolver(pipeliteTaskInstances);

    List<ProcessLauncher.PipeliteTaskInstance> runnableTasks =
        dependencyResolver.getRunnableTasks();

    assertThat(runnableTasks.size()).isOne();
    assertThat(runnableTasks.get(0).getTaskInstance().getTaskName()).isEqualTo("TASK1");
  }

  @Test
  public void testGetRunnableTaskFirstSuccessAllDependOnFirst() {
    List<ProcessLauncher.PipeliteTaskInstance> pipeliteTaskInstances = new ArrayList();

    ProcessBuilder builder =
        new ProcessBuilder(
            UniqueStringGenerator.randomProcessName(), UniqueStringGenerator.randomProcessId());
    ProcessInstance processInstance =
        builder
            .task("TASK1")
            .executor(new SuccessTaskExecutor())
            .taskDependsOn("TASK2", "TASK1")
            .executor(new SuccessTaskExecutor())
            .taskDependsOn("TASK3", "TASK1")
            .executor(new SuccessTaskExecutor())
            .build();

    int taskNumber = 0;
    for (TaskInstance taskInstance : processInstance.getTasks()) {
      PipeliteStage pipeliteStage = new PipeliteStage();
      pipeliteStage.startExecution(taskInstance);
      if (taskNumber == 0) {
        pipeliteStage.endExecution(TaskExecutionResult.success());
      }
      taskNumber++;
      pipeliteTaskInstances.add(
          new ProcessLauncher.PipeliteTaskInstance(taskInstance, pipeliteStage));
    }

    DependencyResolver dependencyResolver = new DependencyResolver(pipeliteTaskInstances);

    List<ProcessLauncher.PipeliteTaskInstance> runnableTasks =
        dependencyResolver.getRunnableTasks();

    assertThat(runnableTasks.size()).isEqualTo(2);
    assertThat(runnableTasks.get(0).getTaskInstance().getTaskName()).isEqualTo("TASK2");
    assertThat(runnableTasks.get(1).getTaskInstance().getTaskName()).isEqualTo("TASK3");
  }

  @Test
  public void testGetRunnableTaskFirstErrorMaxRetriesAllDependOnFirst() {
    List<ProcessLauncher.PipeliteTaskInstance> pipeliteTaskInstances = new ArrayList();

    ProcessBuilder builder =
        new ProcessBuilder(
            UniqueStringGenerator.randomProcessName(), UniqueStringGenerator.randomProcessId());
    ProcessInstance processInstance =
        builder
            .task("TASK1")
            .executor(new SuccessTaskExecutor())
            .taskDependsOn("TASK2", "TASK1")
            .executor(new SuccessTaskExecutor())
            .taskDependsOn("TASK3", "TASK1")
            .executor(new SuccessTaskExecutor())
            .build();

    int taskNumber = 0;
    for (TaskInstance taskInstance : processInstance.getTasks()) {
      PipeliteStage pipeliteStage = new PipeliteStage();
      pipeliteStage.startExecution(taskInstance);
      if (taskNumber == 0) {
        pipeliteStage.endExecution(TaskExecutionResult.error());
        taskInstance.getTaskParameters().setRetries(1);
      }
      taskNumber++;
      pipeliteTaskInstances.add(
          new ProcessLauncher.PipeliteTaskInstance(taskInstance, pipeliteStage));
    }

    DependencyResolver dependencyResolver = new DependencyResolver(pipeliteTaskInstances);

    List<ProcessLauncher.PipeliteTaskInstance> runnableTasks =
        dependencyResolver.getRunnableTasks();

    assertThat(runnableTasks.size()).isEqualTo(0);
  }

  @Test
  public void testGetRunnableTaskFirstErrorNotMaxRetriesAllDependOnFirst() {
    List<ProcessLauncher.PipeliteTaskInstance> pipeliteTaskInstances = new ArrayList();

    ProcessBuilder builder =
        new ProcessBuilder(
            UniqueStringGenerator.randomProcessName(), UniqueStringGenerator.randomProcessId());
    ProcessInstance processInstance =
        builder
            .task("TASK1")
            .executor(new SuccessTaskExecutor())
            .taskDependsOn("TASK2", "TASK1")
            .executor(new SuccessTaskExecutor())
            .taskDependsOn("TASK3", "TASK1")
            .executor(new SuccessTaskExecutor())
            .build();

    int taskNumber = 0;
    for (TaskInstance taskInstance : processInstance.getTasks()) {
      PipeliteStage pipeliteStage = new PipeliteStage();
      pipeliteStage.startExecution(taskInstance);
      if (taskNumber == 0) {
        pipeliteStage.endExecution(TaskExecutionResult.error());
        taskInstance.getTaskParameters().setRetries(3);
      }
      taskNumber++;
      pipeliteTaskInstances.add(
          new ProcessLauncher.PipeliteTaskInstance(taskInstance, pipeliteStage));
    }

    DependencyResolver dependencyResolver = new DependencyResolver(pipeliteTaskInstances);

    List<ProcessLauncher.PipeliteTaskInstance> runnableTasks =
        dependencyResolver.getRunnableTasks();

    assertThat(runnableTasks.size()).isOne();
    assertThat(runnableTasks.get(0).getTaskInstance().getTaskName()).isEqualTo("TASK1");
  }
}
