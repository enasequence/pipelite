package pipelite.resolver;

import pipelite.task.TaskExecutionResult;
import pipelite.task.TaskExecutionResultSerializer;

import java.util.List;

public interface ResultResolver<T> {

  DefaultExceptionResolver DEFAULT_EXCEPTION_RESOLVER = new DefaultExceptionResolver();
  DefaultExitCodeResolver DEFAULT_EXIT_CODE_RESOLVER = new DefaultExitCodeResolver();

  /**
   * @return If cause is null or can't be resolved into a TaskExecutionResult then
   *     TaskExecutionResult.PERMANENT_ERROR is returned.
   */
  TaskExecutionResult resolve(T cause);

  List<TaskExecutionResult> results();

  TaskExecutionResultSerializer<T> serializer();
}
