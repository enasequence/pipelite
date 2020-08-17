package pipelite.resolver;

import pipelite.task.result.TaskExecutionResult;
import pipelite.task.result.serializer.TaskExecutionResultExitCodeSerializer;

import java.util.List;

public class DefaultExceptionResolver implements ExceptionResolver {

  private final ConcreteExceptionResolver resolver;

  public DefaultExceptionResolver() {
    resolver =
        ConcreteExceptionResolver.builder().permanentError(Exception.class, "EXCEPTION").build();
  }

  @Override
  public TaskExecutionResult success() {
    return resolver.success();
  }

  @Override
  public TaskExecutionResult internalError() {
    return resolver.internalError();
  }

  @Override
  public TaskExecutionResult resolveError(Throwable cause) {
    return resolver.resolveError(cause);
  }

  @Override
  public List<TaskExecutionResult> results() {
    return resolver.results();
  }

  @Override
  public TaskExecutionResultExitCodeSerializer<Throwable> exitCodeSerializer() {
    return resolver.exitCodeSerializer();
  }
}
