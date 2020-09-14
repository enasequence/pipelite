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
package pipelite.launcher.process;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import pipelite.UniqueStringGenerator;
import pipelite.entity.TaskEntity;
import pipelite.executor.SuccessTaskExecutor;
import pipelite.process.Process;
import pipelite.process.builder.ProcessBuilder;
import pipelite.task.Task;
import pipelite.task.TaskExecutionResult;

public class DependencyResolverTest {

  @Test
  public void testGetRunnableTaskAllActiveAllDependOnPrevious() {
    List<ProcessLauncher.TaskAndTaskEntity> taskAndTaskEntities = new ArrayList();

    ProcessBuilder builder =
        new ProcessBuilder(
            UniqueStringGenerator.randomProcessName(), UniqueStringGenerator.randomProcessId());
    Process process =
        builder
            .task("TASK1")
            .executor(new SuccessTaskExecutor())
            .taskDependsOnPrevious("TASK2")
            .executor(new SuccessTaskExecutor())
            .taskDependsOnPrevious("TASK3")
            .executor(new SuccessTaskExecutor())
            .build();

    for (Task task : process.getTasks()) {
      TaskEntity taskEntity = new TaskEntity();
      taskEntity.startExecution(task);
      taskAndTaskEntities.add(new ProcessLauncher.TaskAndTaskEntity(task, taskEntity));
    }

    DependencyResolver dependencyResolver = new DependencyResolver(taskAndTaskEntities);

    List<ProcessLauncher.TaskAndTaskEntity> runnableTasks = dependencyResolver.getRunnableTasks();

    assertThat(runnableTasks.size()).isOne();
    assertThat(runnableTasks.get(0).getTask().getTaskName()).isEqualTo("TASK1");
  }

  @Test
  public void testGetRunnableTaskAllActiveAllDependOnFirst() {
    List<ProcessLauncher.TaskAndTaskEntity> taskAndTaskEntities = new ArrayList();

    ProcessBuilder builder =
        new ProcessBuilder(
            UniqueStringGenerator.randomProcessName(), UniqueStringGenerator.randomProcessId());
    Process process =
        builder
            .task("TASK1")
            .executor(new SuccessTaskExecutor())
            .taskDependsOn("TASK2", "TASK1")
            .executor(new SuccessTaskExecutor())
            .taskDependsOn("TASK3", "TASK1")
            .executor(new SuccessTaskExecutor())
            .build();

    for (Task task : process.getTasks()) {
      TaskEntity taskEntity = new TaskEntity();
      taskEntity.startExecution(task);
      taskAndTaskEntities.add(new ProcessLauncher.TaskAndTaskEntity(task, taskEntity));
    }

    DependencyResolver dependencyResolver = new DependencyResolver(taskAndTaskEntities);

    List<ProcessLauncher.TaskAndTaskEntity> runnableTasks = dependencyResolver.getRunnableTasks();

    assertThat(runnableTasks.size()).isOne();
    assertThat(runnableTasks.get(0).getTask().getTaskName()).isEqualTo("TASK1");
  }

  @Test
  public void testGetRunnableTaskFirstSuccessAllDependOnFirst() {
    List<ProcessLauncher.TaskAndTaskEntity> taskAndTaskEntities = new ArrayList();

    ProcessBuilder builder =
        new ProcessBuilder(
            UniqueStringGenerator.randomProcessName(), UniqueStringGenerator.randomProcessId());
    Process process =
        builder
            .task("TASK1")
            .executor(new SuccessTaskExecutor())
            .taskDependsOn("TASK2", "TASK1")
            .executor(new SuccessTaskExecutor())
            .taskDependsOn("TASK3", "TASK1")
            .executor(new SuccessTaskExecutor())
            .build();

    int taskNumber = 0;
    for (Task task : process.getTasks()) {
      TaskEntity taskEntity = new TaskEntity();
      taskEntity.startExecution(task);
      if (taskNumber == 0) {
        taskEntity.endExecution(TaskExecutionResult.success());
      }
      taskNumber++;
      taskAndTaskEntities.add(new ProcessLauncher.TaskAndTaskEntity(task, taskEntity));
    }

    DependencyResolver dependencyResolver = new DependencyResolver(taskAndTaskEntities);

    List<ProcessLauncher.TaskAndTaskEntity> runnableTasks = dependencyResolver.getRunnableTasks();

    assertThat(runnableTasks.size()).isEqualTo(2);
    assertThat(runnableTasks.get(0).getTask().getTaskName()).isEqualTo("TASK2");
    assertThat(runnableTasks.get(1).getTask().getTaskName()).isEqualTo("TASK3");
  }

  @Test
  public void testGetRunnableTaskFirstErrorMaxRetriesAllDependOnFirst() {
    List<ProcessLauncher.TaskAndTaskEntity> taskAndTaskEntities = new ArrayList();

    ProcessBuilder builder =
        new ProcessBuilder(
            UniqueStringGenerator.randomProcessName(), UniqueStringGenerator.randomProcessId());
    Process process =
        builder
            .task("TASK1")
            .executor(new SuccessTaskExecutor())
            .taskDependsOn("TASK2", "TASK1")
            .executor(new SuccessTaskExecutor())
            .taskDependsOn("TASK3", "TASK1")
            .executor(new SuccessTaskExecutor())
            .build();

    int taskNumber = 0;
    for (Task task : process.getTasks()) {
      TaskEntity taskEntity = new TaskEntity();
      taskEntity.startExecution(task);
      if (taskNumber == 0) {
        taskEntity.endExecution(TaskExecutionResult.error());
        task.getTaskParameters().setRetries(1);
      }
      taskNumber++;
      taskAndTaskEntities.add(new ProcessLauncher.TaskAndTaskEntity(task, taskEntity));
    }

    DependencyResolver dependencyResolver = new DependencyResolver(taskAndTaskEntities);

    List<ProcessLauncher.TaskAndTaskEntity> runnableTasks = dependencyResolver.getRunnableTasks();

    assertThat(runnableTasks.size()).isEqualTo(0);
  }

  @Test
  public void testGetRunnableTaskFirstErrorNotMaxRetriesAllDependOnFirst() {
    List<ProcessLauncher.TaskAndTaskEntity> taskAndTaskEntities = new ArrayList();

    ProcessBuilder builder =
        new ProcessBuilder(
            UniqueStringGenerator.randomProcessName(), UniqueStringGenerator.randomProcessId());
    Process process =
        builder
            .task("TASK1")
            .executor(new SuccessTaskExecutor())
            .taskDependsOn("TASK2", "TASK1")
            .executor(new SuccessTaskExecutor())
            .taskDependsOn("TASK3", "TASK1")
            .executor(new SuccessTaskExecutor())
            .build();

    int taskNumber = 0;
    for (Task task : process.getTasks()) {
      TaskEntity taskEntity = new TaskEntity();
      taskEntity.startExecution(task);
      if (taskNumber == 0) {
        taskEntity.endExecution(TaskExecutionResult.error());
        task.getTaskParameters().setRetries(3);
      }
      taskNumber++;
      taskAndTaskEntities.add(new ProcessLauncher.TaskAndTaskEntity(task, taskEntity));
    }

    DependencyResolver dependencyResolver = new DependencyResolver(taskAndTaskEntities);

    List<ProcessLauncher.TaskAndTaskEntity> runnableTasks = dependencyResolver.getRunnableTasks();

    assertThat(runnableTasks.size()).isOne();
    assertThat(runnableTasks.get(0).getTask().getTaskName()).isEqualTo("TASK1");
  }

  @Test
  public void testGetDependentTasksAllDependOnPrevious() {
    List<ProcessLauncher.TaskAndTaskEntity> taskAndTaskEntities = new ArrayList();

    ProcessBuilder builder =
        new ProcessBuilder(
            UniqueStringGenerator.randomProcessName(), UniqueStringGenerator.randomProcessId());
    Process process =
        builder
            .task("TASK1")
            .executor(new SuccessTaskExecutor())
            .taskDependsOnPrevious("TASK2")
            .executor(new SuccessTaskExecutor())
            .taskDependsOnPrevious("TASK3")
            .executor(new SuccessTaskExecutor())
            .taskDependsOnPrevious("TASK4")
            .executor(new SuccessTaskExecutor())
            .build();

    for (Task task : process.getTasks()) {
      TaskEntity taskEntity = new TaskEntity();
      taskAndTaskEntities.add(new ProcessLauncher.TaskAndTaskEntity(task, taskEntity));
    }

    DependencyResolver dependencyResolver = new DependencyResolver(taskAndTaskEntities);

    List<ProcessLauncher.TaskAndTaskEntity> dependentTasks =
        dependencyResolver.getDependentTasks(taskAndTaskEntities.get(0));
    assertThat(dependentTasks.size()).isEqualTo(3);
    assertThat(dependentTasks.get(0).getTask().getTaskName()).isEqualTo("TASK4");
    assertThat(dependentTasks.get(1).getTask().getTaskName()).isEqualTo("TASK3");
    assertThat(dependentTasks.get(2).getTask().getTaskName()).isEqualTo("TASK2");

    dependentTasks = dependencyResolver.getDependentTasks(taskAndTaskEntities.get(1));
    assertThat(dependentTasks.size()).isEqualTo(2);
    assertThat(dependentTasks.get(0).getTask().getTaskName()).isEqualTo("TASK4");
    assertThat(dependentTasks.get(1).getTask().getTaskName()).isEqualTo("TASK3");

    dependentTasks = dependencyResolver.getDependentTasks(taskAndTaskEntities.get(2));
    assertThat(dependentTasks.size()).isEqualTo(1);
    assertThat(dependentTasks.get(0).getTask().getTaskName()).isEqualTo("TASK4");
  }

  @Test
  public void testGetDependentTasksAllDependOnFirst() {
    List<ProcessLauncher.TaskAndTaskEntity> taskAndTaskEntities = new ArrayList();

    ProcessBuilder builder =
        new ProcessBuilder(
            UniqueStringGenerator.randomProcessName(), UniqueStringGenerator.randomProcessId());
    Process process =
        builder
            .task("TASK1")
            .executor(new SuccessTaskExecutor())
            .taskDependsOn("TASK2", "TASK1")
            .executor(new SuccessTaskExecutor())
            .taskDependsOn("TASK3", "TASK1")
            .executor(new SuccessTaskExecutor())
            .taskDependsOn("TASK4", "TASK1")
            .executor(new SuccessTaskExecutor())
            .build();

    for (Task task : process.getTasks()) {
      TaskEntity taskEntity = new TaskEntity();
      taskAndTaskEntities.add(new ProcessLauncher.TaskAndTaskEntity(task, taskEntity));
    }

    DependencyResolver dependencyResolver = new DependencyResolver(taskAndTaskEntities);

    List<ProcessLauncher.TaskAndTaskEntity> dependentTasks =
        dependencyResolver.getDependentTasks(taskAndTaskEntities.get(0));
    assertThat(dependentTasks.size()).isEqualTo(3);
    assertThat(dependentTasks.get(0).getTask().getTaskName()).isEqualTo("TASK2");
    assertThat(dependentTasks.get(1).getTask().getTaskName()).isEqualTo("TASK3");
    assertThat(dependentTasks.get(2).getTask().getTaskName()).isEqualTo("TASK4");

    dependentTasks = dependencyResolver.getDependentTasks(taskAndTaskEntities.get(1));
    assertThat(dependentTasks.size()).isEqualTo(0);

    dependentTasks = dependencyResolver.getDependentTasks(taskAndTaskEntities.get(2));
    assertThat(dependentTasks.size()).isEqualTo(0);

    dependentTasks = dependencyResolver.getDependentTasks(taskAndTaskEntities.get(3));
    assertThat(dependentTasks.size()).isEqualTo(0);
  }
}
