package pipelite.task;

import lombok.Value;
import pipelite.resolver.ResultResolver;

@Value
public class TaskExecutionResultExitCodeSerializer<T> implements TaskExecutionResultSerializer<T> {

  public static final int EXIT_CODE_DEFAULT_SUCCESS = 0;
  public static final int EXIT_CODE_DEFAULT_TRANSIENT_ERROR = 253;
  public static final int EXIT_CODE_DEFAULT_PERMANENT_ERROR = 254;
  public static final int EXIT_CODE_DEFAULT_INTERNAL_ERROR = 255;

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
      return EXIT_CODE_DEFAULT_SUCCESS;
    }
    if (result.isInternalError()) {
      // Unknown internal error.
      return EXIT_CODE_DEFAULT_INTERNAL_ERROR;
    }
    if (result.isTransientError()) {
      // Unknown transient error.
      return EXIT_CODE_DEFAULT_TRANSIENT_ERROR;
    } else {
      // Unknown permanent error.
      return EXIT_CODE_DEFAULT_PERMANENT_ERROR;
    }
  }

  @Override
  public TaskExecutionResult deserialize(int value) {
    if (checkValue(value) && value < resolver.results().size()) {
      return resolver.results().get(value);
    }
    // Unknown result.
    switch (value) {
      case EXIT_CODE_DEFAULT_SUCCESS:
        return TaskExecutionResult.defaultSuccess();
      case EXIT_CODE_DEFAULT_INTERNAL_ERROR:
        return TaskExecutionResult.defaultInternalError();
      case EXIT_CODE_DEFAULT_TRANSIENT_ERROR:
        return TaskExecutionResult.defaultTransientError();
      default:
        return TaskExecutionResult.defaultPermanentError();
    }
  }

  private static boolean checkValue(Integer value) {
    return !(value < 0 || value > 255);
  }
}