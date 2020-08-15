package pipelite.task.result;

import lombok.NonNull;
import lombok.Value;

@Value
public class TaskExecutionResult {

  @NonNull private final String resultName;
  @NonNull private final TaskExecutionResultType resultType;

  public boolean isSuccess() {
    return resultType == TaskExecutionResultType.SUCCESS;
  }

  public boolean isError() {
    return resultType.isError();
  }

  public boolean isTransientError() {
    return resultType == TaskExecutionResultType.TRANSIENT_ERROR;
  }

  public boolean isPermanentError() {
    return resultType == TaskExecutionResultType.PERMANENT_ERROR;
  }

  public boolean isInternalError() {
    return resultType == TaskExecutionResultType.INTERNAL_ERROR;
  }

  public static TaskExecutionResult success() {
    return new TaskExecutionResult("SUCCESS", TaskExecutionResultType.SUCCESS); // TODO: localization
  }

  public static TaskExecutionResult transientError(String resultName) {
    return new TaskExecutionResult(resultName, TaskExecutionResultType.TRANSIENT_ERROR);
  }

  public static TaskExecutionResult permanentError(String resultName) {
    return new TaskExecutionResult(resultName, TaskExecutionResultType.PERMANENT_ERROR);
  }

  public static TaskExecutionResult internalError() {
    return new TaskExecutionResult("INTERNAL ERROR", TaskExecutionResultType.INTERNAL_ERROR);  // TODO: localization
  }
}
