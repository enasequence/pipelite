package pipelite.launcher.process;

import pipelite.entity.PipeliteStage;
import pipelite.task.ConfigurableTaskParameters;
import pipelite.task.TaskExecutionResultType;
import pipelite.task.TaskInstance;

import java.util.ArrayList;
import java.util.List;

public class DependencyResolver {

  private final List<ProcessLauncher.PipeliteTaskInstance> pipeliteTaskInstances;

  public DependencyResolver(List<ProcessLauncher.PipeliteTaskInstance> pipeliteTaskInstances) {
    this.pipeliteTaskInstances = pipeliteTaskInstances;
  }

  public List<ProcessLauncher.PipeliteTaskInstance> getDependentTasks(
      ProcessLauncher.PipeliteTaskInstance from) {
    List<ProcessLauncher.PipeliteTaskInstance> dependentTasks = new ArrayList<>();
    getDependentTasks(dependentTasks, from, false);
    return dependentTasks;
  }

  private void getDependentTasks(
      List<ProcessLauncher.PipeliteTaskInstance> dependentTasks,
      ProcessLauncher.PipeliteTaskInstance from,
      boolean include) {

    for (ProcessLauncher.PipeliteTaskInstance task : pipeliteTaskInstances) {
      if (task.getTaskInstance().equals(from)) {
        continue;
      }

      TaskInstance dependsOn = task.getTaskInstance().getDependsOn();
      if (dependsOn != null
          && dependsOn.getTaskName().equals(from.getTaskInstance().getTaskName())) {
        getDependentTasks(dependentTasks, task, true);
      }
    }

    if (include) {
      dependentTasks.add(from);
    }
  }

  public List<ProcessLauncher.PipeliteTaskInstance> getRunnableTasks() {
    List<ProcessLauncher.PipeliteTaskInstance> runnableTasks = new ArrayList<>();
    for (ProcessLauncher.PipeliteTaskInstance pipeliteTaskInstance : pipeliteTaskInstances) {
      TaskInstance taskInstance = pipeliteTaskInstance.getTaskInstance();
      PipeliteStage pipeliteStage = pipeliteTaskInstance.getPipeliteStage();

      if (isDependsOnTaskCompleted(taskInstance)) {
        switch (pipeliteStage.getResultType()) {
          case NEW:
          case ACTIVE:
            runnableTasks.add(pipeliteTaskInstance);
            break;
          case SUCCESS:
            break;
          case ERROR:
            {
              Integer executionCount = pipeliteStage.getExecutionCount();
              Integer retries = taskInstance.getTaskParameters().getRetries();
              if (retries == null) {
                retries = ConfigurableTaskParameters.DEFAULT_RETRIES;
              }
              if (executionCount < retries) {
                runnableTasks.add(pipeliteTaskInstance);
              }
            }
        }
      }
    }

    return runnableTasks;
  }

  private boolean isDependsOnTaskCompleted(TaskInstance taskInstance) {
    TaskInstance dependsOnTaskInstance = taskInstance.getDependsOn();
    if (dependsOnTaskInstance == null) {
      return true;
    }

    return pipeliteTaskInstances.stream()
            .filter(a -> a.getTaskInstance().equals(dependsOnTaskInstance))
            .findFirst()
            .get()
            .getPipeliteStage()
            .getResultType()
        == TaskExecutionResultType.SUCCESS;
  }
}
