package pipelite.task;

public enum TaskExecutionResultType {
  NEW(false),
  ACTIVE(false),
  SUCCESS(false),
  TRANSIENT_ERROR(true),
  PERMANENT_ERROR(true),
  INTERNAL_ERROR(true),
  RESUME_ERROR(true);

  TaskExecutionResultType(boolean isError) {
    this.isError = isError;
  }

  private final boolean isError;

  public boolean isError() {
    return isError;
  }
}
