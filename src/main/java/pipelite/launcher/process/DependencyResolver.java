package pipelite.launcher.process;

import pipelite.entity.TaskEntity;
import pipelite.task.ConfigurableTaskParameters;
import pipelite.task.TaskExecutionResultType;
import pipelite.task.Task;

import java.util.ArrayList;
import java.util.List;

public class DependencyResolver {

  private final List<ProcessLauncher.TaskAndTaskEntity> taskAndTaskEntities;

  public DependencyResolver(List<ProcessLauncher.TaskAndTaskEntity> taskAndTaskEntities) {
    this.taskAndTaskEntities = taskAndTaskEntities;
  }

  public List<ProcessLauncher.TaskAndTaskEntity> getDependentTasks(
      ProcessLauncher.TaskAndTaskEntity from) {
    List<ProcessLauncher.TaskAndTaskEntity> dependentTasks = new ArrayList<>();
    getDependentTasks(dependentTasks, from, false);
    return dependentTasks;
  }

  private void getDependentTasks(
      List<ProcessLauncher.TaskAndTaskEntity> dependentTasks,
      ProcessLauncher.TaskAndTaskEntity from,
      boolean include) {

    for (ProcessLauncher.TaskAndTaskEntity task : taskAndTaskEntities) {
      if (task.getTask().equals(from)) {
        continue;
      }

      Task dependsOn = task.getTask().getDependsOn();
      if (dependsOn != null
          && dependsOn.getTaskName().equals(from.getTask().getTaskName())) {
        getDependentTasks(dependentTasks, task, true);
      }
    }

    if (include) {
      dependentTasks.add(from);
    }
  }

  public List<ProcessLauncher.TaskAndTaskEntity> getRunnableTasks() {
    List<ProcessLauncher.TaskAndTaskEntity> runnableTasks = new ArrayList<>();
    for (ProcessLauncher.TaskAndTaskEntity taskAndTaskEntity : taskAndTaskEntities) {
      Task task = taskAndTaskEntity.getTask();
      TaskEntity taskEntity = taskAndTaskEntity.getTaskEntity();

      if (isDependsOnTaskCompleted(task)) {
        switch (taskEntity.getResultType()) {
          case NEW:
          case ACTIVE:
            runnableTasks.add(taskAndTaskEntity);
            break;
          case SUCCESS:
            break;
          case ERROR:
            {
              Integer executionCount = taskEntity.getExecutionCount();
              Integer retries = task.getTaskParameters().getRetries();
              if (retries == null) {
                retries = ConfigurableTaskParameters.DEFAULT_RETRIES;
              }
              if (executionCount < retries) {
                runnableTasks.add(taskAndTaskEntity);
              }
            }
        }
      }
    }

    return runnableTasks;
  }

  private boolean isDependsOnTaskCompleted(Task task) {
    Task dependsOnTask = task.getDependsOn();
    if (dependsOnTask == null) {
      return true;
    }

    return taskAndTaskEntities.stream()
            .filter(a -> a.getTask().equals(dependsOnTask))
            .findFirst()
            .get()
            .getTaskEntity()
            .getResultType()
        == TaskExecutionResultType.SUCCESS;
  }
}
