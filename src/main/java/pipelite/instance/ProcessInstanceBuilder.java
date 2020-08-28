package pipelite.instance;

import lombok.Value;
import pipelite.executor.TaskExecutor;
import pipelite.resolver.ResultResolver;

import java.util.ArrayList;
import java.util.List;

@Value
public class ProcessInstanceBuilder {

  private final String processName;
  private final String processId;
  private final int priority;
  private final List<TaskInstance> taskInstances = new ArrayList<>();

  public ProcessInstanceBuilder task(
      String taskName, TaskExecutor executor, ResultResolver resolver) {

    taskInstances.add(
        TaskInstance.builder()
            .processName(processName)
            .processId(processId)
            .taskName(taskName)
            .executor(executor)
            .resolver(resolver)
            .taskParameters(TaskParameters.builder().build())
            .build());
    return this;
  }

  public ProcessInstanceBuilder taskDependsOnPrevious(
      String taskName, TaskExecutor executor, ResultResolver resolver) {

    taskInstances.add(
        TaskInstance.builder()
            .processName(processName)
            .processId(processId)
            .taskName(taskName)
            .executor(executor)
            .resolver(resolver)
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
