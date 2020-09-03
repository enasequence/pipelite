package pipelite.task;

import lombok.Value;
import pipelite.resolver.ResultResolver;

@Value
public class TaskExecutionResultExitCodeSerializer<T> implements TaskExecutionResultSerializer<T> {

  public static final int EXIT_CODE_SUCCESS = 0;
  public static final int EXIT_CODE_ERROR = 1;

  private final ResultResolver<T> resolver;

  @Override
  public int serialize(TaskExecutionResult result) {
    int value = resolver.results().indexOf(result);
    if (checkValue(value)) {
      // Known result.
      return value;
    }
    // Unknown result.
    if (result.isSuccess()) {
      // Unknown success.
      return EXIT_CODE_SUCCESS;
    }
    return EXIT_CODE_ERROR;
  }

  @Override
  public TaskExecutionResult deserialize(int value) {
    if (checkValue(value) && value < resolver.results().size()) {
      return resolver.results().get(value);
    }
    // Unknown result.
    switch (value) {
      case EXIT_CODE_SUCCESS:
        return TaskExecutionResult.success();
      default:
        return TaskExecutionResult.error();
    }
  }

  private static boolean checkValue(Integer value) {
    return !(value < 0 || value > 255);
  }
}
