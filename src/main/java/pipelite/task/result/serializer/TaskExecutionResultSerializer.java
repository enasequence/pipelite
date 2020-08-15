package pipelite.task.result.serializer;

import pipelite.task.result.TaskExecutionResult;

public interface TaskExecutionResultSerializer<T> {

  T serialize(TaskExecutionResult result);

  TaskExecutionResult deserialize(T value);
}
