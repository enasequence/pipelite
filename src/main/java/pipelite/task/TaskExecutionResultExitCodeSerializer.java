package pipelite.task;

import lombok.Value;
import pipelite.resolver.TaskExecutionResultResolver;

import java.util.List;

@Value
public class TaskExecutionResultExitCodeSerializer<T> implements TaskExecutionResultSerializer<T> {

  private final TaskExecutionResultResolver<T> resolver;

  @Override
  public int serialize(TaskExecutionResult result) {
    int value = resolver.results().indexOf(result);
    checkValue(value);
    return value;
  }

  @Override
  public TaskExecutionResult deserialize(int value) {
    checkValue(value);
    List<TaskExecutionResult> results = resolver.results();
    return results.get(value);
  }

  private static void checkValue(Integer value) {
    if (value < 0 || value > 255) {
      throw new IllegalArgumentException("Failed to serialize execution result");
    }
  }
}
