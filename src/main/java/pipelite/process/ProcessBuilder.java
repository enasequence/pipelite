package pipelite.process;

import lombok.Value;
import pipelite.executor.TaskExecutor;
import pipelite.task.TaskInstance;
import pipelite.task.TaskParameters;

import java.util.ArrayList;
import java.util.List;

@Value
public class ProcessBuilder {

  private final String processName;
  private final String processId;
  private final int priority;
  private final List<TaskInstance> taskInstances = new ArrayList<>();

  public ProcessBuilder task(String taskName, TaskExecutor executor) {

    taskInstances.add(
        TaskInstance.builder()
            .processName(processName)
            .processId(processId)
            .taskName(taskName)
            .executor(executor)
            .taskParameters(TaskParameters.builder().build())
            .build());
    return this;
  }

  public ProcessBuilder taskDependsOnPrevious(String taskName, TaskExecutor executor) {

    taskInstances.add(
        TaskInstance.builder()
            .processName(processName)
            .processId(processId)
            .taskName(taskName)
            .executor(executor)
            .taskParameters(TaskParameters.builder().build())
            .dependsOn(taskInstances.get(taskInstances.size() - 1))
            .build());
    return this;
  }

  public ProcessInstance build() {
    return ProcessInstance.builder()
        .processName(processName)
        .processId(processId)
        .priority(priority)
        .tasks(taskInstances)
        .build();
  }
}
