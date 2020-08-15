package pipelite.task.result.resolver;

import pipelite.task.result.ExecutionResult;
import pipelite.task.result.serializer.ExecutionResultExitCodeSerializer;

import java.util.List;

public abstract class ExecutionResultResolver<T> {

  private final ExecutionResult internalError = ExecutionResult.internalError();

  private final ExecutionResultExitCodeSerializer exitCodeSerializer;

  public ExecutionResultResolver() {
    this.exitCodeSerializer = new ExecutionResultExitCodeSerializer(this);
  }

  public abstract ExecutionResult success();

  public abstract ExecutionResult internalError();

  public abstract ExecutionResult resolveError(T cause);

  public abstract List<ExecutionResult> results();

  public ExecutionResult[] resultsArray() {
    return results().stream().toArray(ExecutionResult[]::new);
  }

  public ExecutionResultExitCodeSerializer exitCodeSerializer() {
    return exitCodeSerializer;
  }

  public static final ExecutionResultExceptionResolver DEFAULT_EXCEPTION_RESOLVER =
      ExecutionResultExceptionResolver.builder()
          .permanentError(Exception.class, "EXCEPTION")
          .build();

  /*
  public static final ExecutionResultResolver DEFAULT_EXIT_CODE_RESOLVER =
      ExecutionResultValueResolver.builder(Integer.class)
          .success(0)
          .permanentError(1, "ERROR")
          .build();
   */
}
