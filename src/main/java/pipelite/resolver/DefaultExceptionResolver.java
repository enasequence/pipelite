package pipelite.resolver;

import pipelite.task.TaskExecutionResult;
import pipelite.task.TaskExecutionResultSerializer;

import java.util.List;

public class DefaultExceptionResolver implements TaskExecutionResultResolver<Throwable> {

  public static final String NAME = "pipelite.resolver.DefaultExceptionResolver";

  private final ExceptionResolver resolver;

  public DefaultExceptionResolver() {
    resolver = ExceptionResolver.builder().permanentError(Throwable.class, "EXCEPTION").build();
  }

  @Override
  public TaskExecutionResult resolve(Throwable cause) {
    return resolver.resolve(cause);
  }

  @Override
  public List<TaskExecutionResult> results() {
    return resolver.results();
  }

  @Override
  public TaskExecutionResultSerializer<Throwable> serializer() {
    return resolver.serializer();
  }
}
