package pipelite.task.result;

import lombok.NonNull;
import lombok.Value;

@Value
public class ExecutionResult {

  @NonNull private final String resultName;
  @NonNull private final ExecutionResultType resultType;

  public boolean isSuccess() {
    return resultType == ExecutionResultType.SUCCESS;
  }

  public boolean isError() {
    return resultType.isError();
  }

  public boolean isTransientError() {
    return resultType == ExecutionResultType.TRANSIENT_ERROR;
  }

  public boolean isPermanentError() {
    return resultType == ExecutionResultType.PERMANENT_ERROR;
  }

  public boolean isInternalError() {
    return resultType == ExecutionResultType.INTERNAL_ERROR;
  }

  public static ExecutionResult success() {
    return new ExecutionResult("SUCCESS", ExecutionResultType.SUCCESS); // TODO: localization
  }

  public static ExecutionResult transientError(String resultName) {
    return new ExecutionResult(resultName, ExecutionResultType.TRANSIENT_ERROR);
  }

  public static ExecutionResult permanentError(String resultName) {
    return new ExecutionResult(resultName, ExecutionResultType.PERMANENT_ERROR);
  }

  public static ExecutionResult internalError() {
    return new ExecutionResult("INTERNAL ERROR", ExecutionResultType.INTERNAL_ERROR);  // TODO: localization
  }
}
