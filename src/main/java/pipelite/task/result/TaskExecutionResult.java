package pipelite.task.result;

import lombok.NonNull;
import lombok.Value;

@Value
public class TaskExecutionResult {

  @NonNull private final String resultName;
  @NonNull private final TaskoExecutionResultType resultType;

  public boolean isSuccess() {
    return resultType == TaskoExecutionResultType.SUCCESS;
  }

  public boolean isError() {
    return resultType.isError();
  }

  public boolean isTransientError() {
    return resultType == TaskoExecutionResultType.TRANSIENT_ERROR;
  }

  public boolean isPermanentError() {
    return resultType == TaskoExecutionResultType.PERMANENT_ERROR;
  }

  public boolean isInternalError() {
    return resultType == TaskoExecutionResultType.INTERNAL_ERROR;
  }

  public static TaskExecutionResult success() {
    return new TaskExecutionResult("SUCCESS", TaskoExecutionResultType.SUCCESS); // TODO: localization
  }

  public static TaskExecutionResult transientError(String resultName) {
    return new TaskExecutionResult(resultName, TaskoExecutionResultType.TRANSIENT_ERROR);
  }

  public static TaskExecutionResult permanentError(String resultName) {
    return new TaskExecutionResult(resultName, TaskoExecutionResultType.PERMANENT_ERROR);
  }

  public static TaskExecutionResult internalError() {
    return new TaskExecutionResult("INTERNAL ERROR", TaskoExecutionResultType.INTERNAL_ERROR);  // TODO: localization
  }
}
