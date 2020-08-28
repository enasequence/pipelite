package pipelite.instance;

import lombok.Value;
import pipelite.configuration.TaskConfiguration;
import pipelite.configuration.TaskConfigurationEx;
import pipelite.executor.TaskExecutor;

import java.util.ArrayList;
import java.util.List;

@Value
public class ProcessInstanceBuilder {

  private final String processName;
  private final String processId;
  private final int priority;
  private final List<TaskInstance> taskInstances = new ArrayList<>();

  public ProcessInstanceBuilder task(String taskName, TaskParameters taskParameters) {
    taskInstances.add(
        TaskInstance.builder()
            .processName(processName)
            .processId(processId)
            .taskName(taskName)
            .taskExecutor(taskParameters.getTaskExecutor())
            .taskParameters(taskParameters)
            .build());
    return this;
  }

  public ProcessInstanceBuilder task(String taskName, TaskExecutor taskExecutor) {
    TaskConfigurationEx taskConfiguration = new TaskConfigurationEx(new TaskConfiguration());
    taskConfiguration.setTaskExecutor(taskExecutor);

    taskInstances.add(
        TaskInstance.builder()
            .processName(processName)
            .processId(processId)
            .taskName(taskName)
            .taskExecutor(taskExecutor)
            .taskParameters(taskConfiguration)
            .build());
    return this;
  }

  public ProcessInstanceBuilder taskDependsOnPrevious(
      String taskName, TaskParameters taskParameters) {
    taskInstances.add(
        TaskInstance.builder()
            .processName(processName)
            .processId(processId)
            .taskName(taskName)
            .taskExecutor(taskParameters.getTaskExecutor())
            .taskParameters(taskParameters)
            .dependsOn(taskInstances.get(taskInstances.size() - 1))
            .build());
    return this;
  }

  public ProcessInstanceBuilder taskDependsOnPrevious(String taskName, TaskExecutor taskExecutor) {
    TaskConfigurationEx taskConfiguration = new TaskConfigurationEx(new TaskConfiguration());
    taskConfiguration.setTaskExecutor(taskExecutor);

    taskInstances.add(
        TaskInstance.builder()
            .processName(processName)
            .processId(processId)
            .taskName(taskName)
            .taskExecutor(taskExecutor)
            .taskParameters(taskConfiguration)
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
