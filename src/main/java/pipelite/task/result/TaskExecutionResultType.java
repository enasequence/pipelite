package pipelite.task.result;

public enum TaskExecutionResultType {
    SUCCESS(false),
    TRANSIENT_ERROR(true),
    PERMANENT_ERROR(true);

    TaskExecutionResultType(boolean isError) {
        this.isError = isError;
    }

    private final boolean isError;

    public boolean isError() {
        return isError;
    }
}
