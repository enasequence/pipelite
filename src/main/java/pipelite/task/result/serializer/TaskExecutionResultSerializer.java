package pipelite.task.result.serializer;

import pipelite.task.result.TaskExecutionResult;

public interface TaskExecutionResultSerializer<T> {

  int serialize(TaskExecutionResult result);

  TaskExecutionResult deserialize(int value);
}
