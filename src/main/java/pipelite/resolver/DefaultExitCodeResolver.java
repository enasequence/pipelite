package pipelite.resolver;

import pipelite.task.TaskExecutionResult;
import pipelite.task.TaskExecutionResultSerializer;

import java.util.List;

public class DefaultExitCodeResolver implements TaskExecutionResultResolver<Integer> {

  public static final String NAME = "pipelite.resolver.DefaultExitCodeResolver";

  private final ExitCodeResolver resolver;

  public DefaultExitCodeResolver() {
    resolver = ExitCodeResolver.builder().success(Integer.valueOf(0)).build();
  }

  @Override
  public TaskExecutionResult resolve(Integer cause) {
    return resolver.resolve(cause);
  }

  @Override
  public List<TaskExecutionResult> results() {
    return resolver.results();
  }

  @Override
  public TaskExecutionResultSerializer<Integer> serializer() {
    return resolver.serializer();
  }
}
