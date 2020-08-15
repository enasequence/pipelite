package pipelite.task.result;

public enum ExecutionResultType {
  SUCCESS(false),
  TRANSIENT_ERROR(true),
  PERMANENT_ERROR(true),
  INTERNAL_ERROR(true);

  ExecutionResultType(boolean isError) {
    this.isError = isError;
  }

  private final boolean isError;

  public boolean isError() {
    return isError;
  }
}
