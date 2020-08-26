package pipelite.instance;

import lombok.Value;
import pipelite.configuration.TaskConfiguration;
import pipelite.configuration.TaskConfigurationEx;
import pipelite.task.TaskFactory;

import java.util.ArrayList;
import java.util.List;

@Value
public class ProcessInstanceBuilder {

  private final String processName;
  private final String processId;
  private final int priority;
  private final List<TaskInstance> taskInstances = new ArrayList<>();

  public ProcessInstanceBuilder task(
      String taskName, TaskFactory taskFactory, TaskParameters taskParameters) {
    taskInstances.add(
        TaskInstance.builder()
            .processName(processName)
            .processId(processId)
            .taskName(taskName)
            .taskFactory(taskFactory)
            .taskParameters(taskParameters)
            .build());
    return this;
  }

  public ProcessInstanceBuilder task(String taskName, TaskFactory taskFactory) {
    taskInstances.add(
        TaskInstance.builder()
            .processName(processName)
            .processId(processId)
            .taskName(taskName)
            .taskFactory(taskFactory)
            .taskParameters(new TaskConfigurationEx(new TaskConfiguration()))
            .build());
    return this;
  }

  public ProcessInstanceBuilder taskDependsOnPrevious(
      String taskName, TaskFactory taskFactory, TaskParameters taskParameters) {
    taskInstances.add(
        TaskInstance.builder()
            .processName(processName)
            .processId(processId)
            .taskName(taskName)
            .taskFactory(taskFactory)
            .taskParameters(taskParameters)
            .dependsOn(taskInstances.get(taskInstances.size() - 1))
            .build());
    return this;
  }

  public ProcessInstanceBuilder taskDependsOnPrevious(String taskName, TaskFactory taskFactory) {
    taskInstances.add(
        TaskInstance.builder()
            .processName(processName)
            .processId(processId)
            .taskName(taskName)
            .taskFactory(taskFactory)
            .taskParameters(new TaskConfigurationEx(new TaskConfiguration()))
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
