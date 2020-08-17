package pipelite.resolver;

import pipelite.task.result.serializer.TaskExecutionResultExitCodeSerializer;

public abstract class AbstractResolver<T> implements TaskExecutionResultResolver<T> {

  private final TaskExecutionResultExitCodeSerializer<T> exitCodeSerializer;

  public AbstractResolver() {
    this.exitCodeSerializer = new TaskExecutionResultExitCodeSerializer(this);
  }

  @Override
  public TaskExecutionResultExitCodeSerializer<T> exitCodeSerializer() {
    return exitCodeSerializer;
  }
}
