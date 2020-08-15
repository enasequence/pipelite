package pipelite.task.result.serializer;

import pipelite.task.result.ExecutionResult;

public interface ExecutionResultSerializer<T> {

  T serialize(ExecutionResult result);

  ExecutionResult deserialize(T value);
}
