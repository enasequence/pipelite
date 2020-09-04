package pipelite.task;

public enum TaskExecutionResultType {
  NEW(false),
  ACTIVE(false),
  SUCCESS(false),
  ERROR(true);

  TaskExecutionResultType(boolean isError) {
    this.isError = isError;
  }

  private final boolean isError;

  public boolean isError() {
    return isError;
  }
}
