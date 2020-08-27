package pipelite.resolver;

import pipelite.task.result.TaskExecutionResult;
import pipelite.task.result.serializer.TaskExecutionResultSerializer;

import java.util.List;

public interface TaskExecutionResultResolver<T> {

  /** Null cause resolves to success. */
  TaskExecutionResult resolve(T cause);

  List<TaskExecutionResult> results();

  TaskExecutionResultSerializer<T> serializer();
}
