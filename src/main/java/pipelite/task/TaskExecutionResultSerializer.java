package pipelite.task;

public interface TaskExecutionResultSerializer<T> {

  int serialize(TaskExecutionResult result);

  TaskExecutionResult deserialize(int value);
}
