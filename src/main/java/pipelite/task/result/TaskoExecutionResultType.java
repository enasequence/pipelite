package pipelite.task.result;

public enum TaskoExecutionResultType {
  SUCCESS(false),
  TRANSIENT_ERROR(true),
  PERMANENT_ERROR(true),
  INTERNAL_ERROR(true);

  TaskoExecutionResultType(boolean isError) {
    this.isError = isError;
  }

  private final boolean isError;

  public boolean isError() {
    return isError;
  }
}
