package pipelite.task.result.resolver;

import pipelite.task.result.TaskExecutionResult;
import pipelite.task.result.serializer.TaskExecutionResultExitCodeSerializer;

import java.util.List;

public abstract class TaskExecutionResultResolver<T> {

  private final TaskExecutionResult internalError = TaskExecutionResult.internalError();

  private final TaskExecutionResultExitCodeSerializer<T> exitCodeSerializer;

  public TaskExecutionResultResolver() {
    this.exitCodeSerializer = new TaskExecutionResultExitCodeSerializer(this);
  }

  public abstract TaskExecutionResult success();

  public abstract TaskExecutionResult internalError();

  public abstract TaskExecutionResult resolveError(T cause);

  public abstract List<TaskExecutionResult> results();

  public TaskExecutionResult[] resultsArray() {
    return results().toArray(new TaskExecutionResult[0]);
  }

  public TaskExecutionResultExitCodeSerializer<T> exitCodeSerializer() {
    return exitCodeSerializer;
  }

  public static final TaskExecutionResultExceptionResolver DEFAULT_EXCEPTION_RESOLVER =
      TaskExecutionResultExceptionResolver.builder()
          .permanentError(Exception.class, "EXCEPTION")
          .build();
}
