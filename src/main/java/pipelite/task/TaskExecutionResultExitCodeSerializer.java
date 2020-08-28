package pipelite.task;

import lombok.Value;
import pipelite.resolver.TaskExecutionResultResolver;

import java.util.List;

@Value
public class TaskExecutionResultExitCodeSerializer<T> implements TaskExecutionResultSerializer<T> {

  private final TaskExecutionResultResolver<T> resolver;

  public static final int INTERNAL_ERROR_EXIT_CODE = 255;

  @Override
  public int serialize(TaskExecutionResult result) {
    int value = resolver.results().indexOf(result);
    if (!checkValue(value)) {
      return INTERNAL_ERROR_EXIT_CODE;
    }
    return value;
  }

  @Override
  public TaskExecutionResult deserialize(int value) {
    if (!checkValue(value) || value >= resolver.results().size()) {
      return TaskExecutionResult.internalError();
    }
    return resolver.results().get(value);
  }

  private static boolean checkValue(Integer value) {
    return !(value < 0 || value > 255);
  }
}
